from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

import gspread
import pytz
from google.oauth2.service_account import Credentials
from gspread import Client, Spreadsheet, Worksheet
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from pandas import DataFrame


T = TypeVar("T")


# =============================================================================
# Retry wrapper for gspread API calls
# =============================================================================
class RetryingMethod:
    """
    Callable wrapper that retries a function on transient gspread API errors.

    Retries on HTTP 500/503 with exponential backoff and on 429 with a fixed wait.
    Any other APIError is re-raised immediately.
    """

    def __init__(
        self,
        function: Callable[..., T],
        retries: int = 10,
        delay: float = 5.0,
        backoff_factor: float = 2.0,
        rate_limit_sleep_seconds: float = 60.0,
    ) -> None:
        self._function = function
        self._retries = retries
        self._delay = delay
        self._backoff_factor = backoff_factor
        self._rate_limit_sleep_seconds = rate_limit_sleep_seconds

    def __call__(self, *args: Any, **kwargs: Any) -> T:
        attempt = 0
        while True:
            try:
                return self._function(*args, **kwargs)
            except APIError as e:
                # gspread.APIError exposes an HTTP response with a status_code.
                status = getattr(e.response, "status_code", None)

                # Non-HTTP or unknown: re-raise.
                if status is None:
                    raise

                attempt += 1
                if attempt > self._retries:
                    raise

                if status in (500, 503):
                    sleep_s = self._delay * (self._backoff_factor ** (attempt - 1))
                    print(
                        f"APIError {status}: retry {attempt}/{self._retries} in {sleep_s:.1f}s",
                        flush=True,
                    )
                    time.sleep(sleep_s)
                    continue

                if status == 429:
                    print(
                        f"APIError 429: retry {attempt}/{self._retries} in {self._rate_limit_sleep_seconds:.0f}s",
                        flush=True,
                    )
                    time.sleep(self._rate_limit_sleep_seconds)
                    continue

                raise


# =============================================================================
# Resilient wrapper that propagates retry logic to returned Spreadsheet/Worksheet
# =============================================================================
class ResilientGspreadClient:
    """
    Wrapper around a gspread-like object that applies retry logic to method calls.

    - Any callable attribute is executed via RetryingMethod.
    - If the result is a Spreadsheet or Worksheet, it is wrapped again so retries
      propagate down the object graph.
    - If the result is an iterable of such objects, those elements are wrapped.
    """

    def __init__(
        self,
        base_client_instance: Any,
        retries: int = 10,
        delay: float = 5.0,
        backoff_factor: float = 2.0,
        propagating_classes: Optional[Sequence[Type[Any]]] = None,
    ) -> None:
        self._base = base_client_instance
        self._retries = retries
        self._delay = delay
        self._backoff_factor = backoff_factor
        self._propagating_classes = (
            list(propagating_classes)
            if propagating_classes
            else [Spreadsheet, Worksheet]
        )

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._base, name)

        if not callable(attr):
            return attr

        def wrapped_method(*args: Any, **kwargs: Any) -> Any:
            call = RetryingMethod(
                attr,
                retries=self._retries,
                delay=self._delay,
                backoff_factor=self._backoff_factor,
            )
            result = call(*args, **kwargs)

            if self._should_wrap(result):
                return ResilientGspreadClient(
                    result,
                    retries=self._retries,
                    delay=self._delay,
                    backoff_factor=self._backoff_factor,
                    propagating_classes=self._propagating_classes,
                )

            if self._is_iterable(result):
                return self._wrap_iterable(result)

            return result

        return wrapped_method

    def _should_wrap(self, obj: Any) -> bool:
        return any(isinstance(obj, cls) for cls in self._propagating_classes)

    def _is_iterable(self, obj: Any) -> bool:
        # Exclude strings/bytes; treat everything with __iter__ as iterable.
        return not isinstance(obj, (str, bytes)) and hasattr(obj, "__iter__")

    def _wrap_iterable(self, it: Iterable[Any]) -> Iterator[Any]:
        for item in it:
            if self._should_wrap(item):
                yield ResilientGspreadClient(
                    item,
                    retries=self._retries,
                    delay=self._delay,
                    backoff_factor=self._backoff_factor,
                    propagating_classes=self._propagating_classes,
                )
            else:
                yield item


# =============================================================================
# Spreadsheet-as-table with schema metadata
# =============================================================================
class SchemaGSheet:
    """
    Treats a Spreadsheet as a table with a companion schema worksheet.

    Worksheets:
      - table_data: actual data
      - schema: column name, dtype, description

    Mode:
      - 'w': clears sheets on enter; append-style writes with tracked start row
      - 'r': reads and casts using schema
    """

    def __init__(
        self,
        sh: Spreadsheet,
        mode: str,
        column_descriptions: Optional[Mapping[str, str]] = None,
    ) -> None:
        if mode not in ("r", "w"):
            raise ValueError("mode must be 'r' or 'w'")

        self.sh = sh
        self.mode = mode
        self.column_descriptions: Dict[str, str] = dict(column_descriptions or {})

        # Data worksheet
        self.data_sheet_name = "table_data"
        try:
            # Ensure first worksheet is titled consistently.
            self.sh.get_worksheet(0).update_title(self.data_sheet_name)
            self.data_sheet = self.sh.worksheet(self.data_sheet_name)
        except WorksheetNotFound:
            self.data_sheet = self.sh.add_worksheet(
                self.data_sheet_name, rows=10_000, cols=20
            )

        # Schema worksheet
        self.schema_sheet_name = "schema"
        try:
            self.schema_sheet = self.sh.worksheet(self.schema_sheet_name)
        except WorksheetNotFound:
            self.schema_sheet = self.sh.add_worksheet(
                self.schema_sheet_name, rows=100, cols=20
            )

        self.df_schema: Optional[DataFrame] = None
        self.start_row: int = 1
        self.write_header: bool = True

    def __enter__(self) -> SchemaGSheet:
        self.start_row = 1
        self.write_header = True

        if self.mode == "w":
            self.data_sheet.clear()
            self.schema_sheet.clear()

        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        # No external resources to close; keep as no-op for context manager ergonomics.
        return None

    def write_dataframe(self, df: DataFrame) -> None:
        """Append a DataFrame to table_data; initialize schema on first write."""
        if self.mode != "w":
            raise PermissionError("SchemaGSheet is not opened in write mode.")

        if df.empty:
            return

        # Ensure enough rows (accounting for header row if writing header).
        needed_rows = (
            df.shape[0]
            + int(self.write_header)
            + self.start_row
            - self.data_sheet.row_count
        )
        if needed_rows > 0:
            self.data_sheet.add_rows(needed_rows)
            self.data_sheet = self.sh.worksheet(self.data_sheet_name)

        # Ensure enough columns.
        needed_cols = df.shape[1] - self.data_sheet.col_count
        if needed_cols > 0:
            self.data_sheet.add_cols(needed_cols)
            self.data_sheet = self.sh.worksheet(self.data_sheet_name)

        set_with_dataframe(
            self.data_sheet,
            dataframe=df,
            row=self.start_row,
            include_column_header=self.write_header,
        )

        # Write schema once.
        if self.df_schema is None:
            self.df_schema = DataFrame(
                {
                    "name": df.columns,
                    "dtype": [str(dtype) for dtype in df.dtypes],
                    "description": [
                        self.column_descriptions.get(c, "") for c in df.columns
                    ],
                }
            )
            set_with_dataframe(self.schema_sheet, self.df_schema)

        self.start_row += len(df) + int(self.write_header)
        self.write_header = False

    def read_dataframe(self) -> DataFrame:
        """Read table_data and cast columns using schema sheet dtypes."""
        if self.mode != "r":
            raise PermissionError("SchemaGSheet is not opened in read mode.")

        raw_data = (
            get_as_dataframe(self.data_sheet, na_values=[""])
            .dropna(axis=0, how="all")
            .dropna(axis=1, how="all")
        )

        df_schema = (
            get_as_dataframe(self.schema_sheet, na_values=[""], dtype=str)
            .dropna(axis=0, how="all")
            .dropna(axis=1, how="all")
        )

        dtype_dict: Dict[str, str] = {
            row["name"]: row["dtype"] for _, row in df_schema.iterrows()
        }

        for col, dtype in dtype_dict.items():
            if col not in raw_data.columns:
                continue
            try:
                raw_data[col] = raw_data[col].astype(dtype)
            except (ValueError, TypeError):
                print(
                    f"Warning: could not cast column '{col}' to '{dtype}'", flush=True
                )

        return raw_data


# =============================================================================
# Table versioning helpers
# =============================================================================
def return_timestamp(tz_name: str = "America/New_York") -> str:
    """Timestamp string used in archived sheet names."""
    tz = pytz.timezone(tz_name)
    return datetime.now(tz).strftime("%y-%m-%d_%H:%M")


class TableVersionRef:
    """Computed names for a table in primary/temp/archive lifecycles."""

    def __init__(
        self, table_name: str, env: str = "prod", timestamp: Optional[str] = None
    ) -> None:
        self.table_name = table_name
        self.env = env
        self.tstamp = timestamp or return_timestamp()
        self.env_suffix = "" if self.env == "prod" else f"_{env}"

        self._dict: Dict[str, str] = {
            "primary": f"{self.table_name}{self.env_suffix}",
            "temp": f"__{self.table_name}_TEMP{self.env_suffix}",
            "archive": f"_{self.table_name}{self.env_suffix}_{self.tstamp}",
        }

    def __getitem__(self, key: str) -> Optional[str]:
        return self._dict.get(key)


class TableVersionManager:
    """
    Creates and promotes Sheets used as warehouse tables, with optional lifecycle management.

    If lifecycle_management=True, writes go to a TEMP sheet and are later promoted.
    """

    def __init__(
        self,
        gclient: Any,
        env_folder_ids: Mapping[str, str],
        env: str = "prod",
        timestamp: Optional[str] = None,
    ) -> None:
        self.gc = gclient
        self.env_folder_ids = dict(env_folder_ids)
        self.env = env
        self.timestamp = timestamp or return_timestamp()
        self.temp_tables: Dict[str, Tuple[TableVersionRef, str]] = {}

    def create_table(
        self, table_name: str, lifecycle_management: bool = True
    ) -> Spreadsheet:
        """
        Create a new Sheet in the env folder.

        If lifecycle_management=True, create a TEMP sheet and register it for promotion.
        Otherwise create/return a primary-named sheet.
        """
        table_ref = TableVersionRef(table_name, env=self.env, timestamp=self.timestamp)

        folder_id = self.env_folder_ids.get(self.env)
        if not folder_id:
            raise ValueError(f"No folder ID specified for environment '{self.env}'.")

        if lifecycle_management:
            sh = self.gc.create(table_ref["temp"], folder_id=folder_id)
            self.temp_tables[table_name] = (table_ref, sh.url)
            return sh

        return self.gc.create(table_ref["primary"], folder_id=folder_id)

    def retrieve_latest(self, table_name: str) -> Spreadsheet:
        """
        Return the TEMP version if present; otherwise return the primary version.
        """
        table_ref = TableVersionRef(table_name, env=self.env, timestamp=self.timestamp)

        if table_name in self.temp_tables:
            return self.gc.open(table_ref["temp"])

        try:
            return self.gc.open(table_ref["primary"])
        except SpreadsheetNotFound as e:
            raise ValueError(
                f"No table found for '{table_name}' in env='{self.env}'."
            ) from e

    def promote_temp_tables(self, archive_previous: bool = True) -> None:
        """
        Promote all registered TEMP tables to primary; optionally archive existing primary versions.
        """
        while self.temp_tables:
            table_name, (table_ref, sheet_url) = self.temp_tables.popitem()

            try:
                previous_sheet = self.gc.open(table_ref["primary"])
            except SpreadsheetNotFound:
                previous_sheet = None

            # Promote temp -> primary
            new_sheet = self.gc.open_by_url(sheet_url)
            new_sheet.update_title(table_ref["primary"])

            if previous_sheet is None:
                continue

            if archive_previous:
                previous_sheet.update_title(table_ref["archive"])
            else:
                self.gc.del_spreadsheet(previous_sheet.id)


# =============================================================================
# High-level data connector
# =============================================================================
class DataConnector:
    """
    High-level interface to read/write warehouse tables and prep tables backed by Google Sheets.

    - Uses a service account to authenticate.
    - Wraps gspread client in ResilientGspreadClient for retry behavior.
    - Writes warehouse tables using SchemaGSheet + TableVersionManager.
    """

    def __init__(
        self,
        auth_credentials: str,
        warehouse_folder_ids: Mapping[str, str],
        env: str = "prod",
        retries: int = 10,
        delay: float = 5.0,
        backoff_factor: float = 2.0,
        **kwargs: Any,
    ) -> None:
        self.env = env
        self.warehouse_folder_ids = dict(warehouse_folder_ids)

        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        creds = Credentials.from_service_account_info(
            json.loads(auth_credentials)
        ).with_scopes(scopes)
        base_gc = gspread.authorize(creds)

        self.gc = ResilientGspreadClient(
            base_gc,
            retries=retries,
            delay=delay,
            backoff_factor=backoff_factor,
        )

        self.version_manager = TableVersionManager(
            self.gc,
            env_folder_ids=self.warehouse_folder_ids,
            env=self.env,
            **kwargs,
        )

    def put_warehouse_table(
        self,
        table_name: str,
        data: Union[DataFrame, Iterable[DataFrame]],
        lifecycle_management: bool = True,
    ) -> None:
        """
        Write a DataFrame (or iterable of DataFrames) to a warehouse table.

        If lifecycle_management=True, writes to a TEMP sheet registered for promotion.
        """
        sh = self.version_manager.create_table(
            table_name, lifecycle_management=lifecycle_management
        )

        # Prefer the DataFrame path; fall back to iterables.
        if isinstance(data, DataFrame):
            with SchemaGSheet(sh, "w") as schema_sheet:
                schema_sheet.write_dataframe(data)
            return

        with SchemaGSheet(sh, "w") as schema_sheet:
            wrote_any = False
            for df in data:
                wrote_any = True
                schema_sheet.write_dataframe(df)

        if not wrote_any:
            # Make empty-iterable writes a no-op rather than silently creating a blank schema sheet.
            return

    def get_warehouse_table(
        self, table_name: str, return_url: bool = False
    ) -> Union[DataFrame, Tuple[DataFrame, str]]:
        """
        Load the latest version of a warehouse table into a DataFrame.
        """
        try:
            sh = self.version_manager.retrieve_latest(table_name)
        except SpreadsheetNotFound as e:
            print(f"Spreadsheet not found: {table_name}", flush=True)
            raise

        with SchemaGSheet(sh, "r") as schema_sheet:
            df = schema_sheet.read_dataframe()

        return (df, sh.url) if return_url else df

    def promote_temp_tables(self, archive_previous: bool = True) -> None:
        """Promote all TEMP tables to primary; optionally archive old primary versions."""
        self.version_manager.promote_temp_tables(archive_previous=archive_previous)

    def get_prep_table(
        self,
        sheet_name: str,
        worksheet_name: Optional[str] = None,
        return_url: bool = False,
    ) -> Union[DataFrame, Tuple[DataFrame, str]]:
        """
        Load a worksheet from a prep sheet as a DataFrame.

        worksheet_name:
          - None: first worksheet
          - numeric string/int-like: treated as worksheet index
          - otherwise: treated as worksheet title
        """
        try:
            sh = self.gc.open(sheet_name)
        except SpreadsheetNotFound:
            print(f"Spreadsheet not found: {sheet_name}", flush=True)
            raise

        try:
            if worksheet_name is None:
                worksheet = sh.get_worksheet(0)
            else:
                worksheet = sh.get_worksheet(int(worksheet_name))
        except (WorksheetNotFound, ValueError):
            worksheet = sh.worksheet(worksheet_name)

        df = DataFrame(worksheet.get_all_records()).convert_dtypes()
        return (df, sh.url) if return_url else df

    def iter_prep_tables(self, sheet_name: str) -> Iterator[Tuple[str, DataFrame]]:
        """
        Yield (worksheet_title, DataFrame) for each worksheet in a sheet.
        """
        sh = self.gc.open(sheet_name)
        for worksheet in sh.worksheets():
            df = DataFrame(worksheet.get_all_records()).convert_dtypes()
            yield worksheet.title, df
