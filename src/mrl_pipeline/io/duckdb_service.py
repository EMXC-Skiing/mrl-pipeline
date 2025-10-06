"""DuckDB-backed implementation of WarehouseService."""

from __future__ import annotations

import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional, Tuple, Union

from duckdb import DuckDBPyConnection
from pandas import DataFrame

from mrl_pipeline.io.database_connectors import (
    DatabaseConnector,
    LocalDuckDBConnector,
    MotherDuckConnector,
)
from mrl_pipeline.io.warehouse_service import (
    TableVersionRef,
    WarehouseService,
    _default_timestamp,
)
from mrl_pipeline.utils import duckdb_path, sanitize_table_name


class DuckDBWarehouseService(WarehouseService):
    """WarehouseService implementation backed by DuckDB tables."""

    def __init__(
        self,
        *,
        auth_credentials: Optional[Any] = None,
        env: Optional[str] = "prod",
        connector: Optional[DatabaseConnector] = None,
        database_path: Optional[Union[str, Path]] = None,
        schema: str = "main",
        timestamp: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(auth_credentials=auth_credentials, env=env, **kwargs)

        extra_timestamp = self.extra_options.pop("timestamp", None)
        schema_token = sanitize_table_name(schema) if schema else ""
        self.schema = schema_token or "main"
        self.env_token = sanitize_table_name(self.env)
        self.env_suffix = (
            ""
            if self.env.lower() == "prod"
            else f"_{self.env_token or self.env.lower()}"
        )
        self.timestamp = timestamp or extra_timestamp or _default_timestamp()

        self.connector = self._resolve_connector(
            connector=connector,
            auth_credentials=auth_credentials,
            database_path=database_path,
            extra_options=self.extra_options,
        )
        self.temp_tables: Dict[str, TableVersionRef] = {}
        self._temp_lock = threading.RLock()
        self._location = self._resolve_location()

    def put_warehouse_table(
        self,
        table_name: str,
        data: Union[DataFrame, Iterable[DataFrame]],
        *,
        lifecycle_management: bool = True,
    ) -> None:
        table_ref = self._table_ref(table_name)
        target_name = (
            table_ref["temp"] if lifecycle_management else table_ref["primary"]
        )

        wrote_any = False
        with self._connection() as conn:
            first_chunk = True
            for chunk in self._iter_dataframes(data):
                wrote_any = True
                self._write_dataframe(conn, chunk, target_name, append=not first_chunk)
                first_chunk = False

        if not wrote_any:
            raise ValueError("`data` must yield at least one DataFrame.")

        with self._temp_lock:
            if lifecycle_management:
                self.temp_tables[table_ref.base] = table_ref
            else:
                self.temp_tables.pop(table_ref.base, None)

    def get_warehouse_table(
        self,
        table_name: str,
        *,
        return_url: bool = False,
    ) -> Union[DataFrame, Tuple[DataFrame, str]]:
        table_ref = self._table_ref(table_name)
        primary_name = table_ref["primary"]

        with self._connection() as conn:
            if not self._table_exists(conn, primary_name):
                raise ValueError(
                    f"No table found for '{table_name}' in the {self.env} environment.",
                )
            df = self._read_table(conn, primary_name)

        if return_url:
            return df, self._table_url(primary_name)
        return df

    def promote_temp_tables(self, *, archive_previous: bool = True) -> None:
        with self._temp_lock:
            if not self.temp_tables:
                return
            pending = list(self.temp_tables.items())

        with self._connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                for base_name, table_ref in pending:
                    primary_name = table_ref["primary"]
                    temp_name = table_ref["temp"]
                    archive_name = table_ref["archive"]

                    if not self._table_exists(conn, temp_name):
                        raise ValueError(
                            f"Temporary table '{temp_name}' missing for "
                            f"'{table_ref.original_name}'.",
                        )

                    if self._table_exists(conn, primary_name):
                        if archive_previous:
                            if self._table_exists(conn, archive_name):
                                conn.execute(
                                    f"DROP TABLE {self._qualified(archive_name)}"
                                )
                            conn.execute(
                                f"ALTER TABLE {self._qualified(primary_name)} "
                                f"RENAME TO {self._quote_identifier(archive_name)}",
                            )
                        else:
                            conn.execute(f"DROP TABLE {self._qualified(primary_name)}")

                    conn.execute(
                        f"ALTER TABLE {self._qualified(temp_name)} "
                        f"RENAME TO {self._quote_identifier(primary_name)}",
                    )

                conn.execute("COMMIT")
                # After commit, purge promoted entries
                with self._temp_lock:
                    for base_name, _ in pending:
                        self.temp_tables.pop(base_name, None)
            except Exception:  # noqa: BLE001
                conn.execute("ROLLBACK")
                raise

    def get_prep_table(
        self,
        sheet_name: str,
        *,
        worksheet_name: Optional[str] = None,
        return_url: bool = False,
    ) -> Union[DataFrame, Tuple[DataFrame, str]]:
        raise NotImplementedError(
            "DuckDBWarehouseService does not support loading prep tables.",
        )

    def iter_prep_tables(self, sheet_name: str) -> Iterable[Tuple[str, DataFrame]]:
        raise NotImplementedError(
            "DuckDBWarehouseService does not support iterating prep tables.",
        )

    def _resolve_connector(
        self,
        *,
        connector: Optional[DatabaseConnector],
        auth_credentials: Optional[Any],
        database_path: Optional[Union[str, Path]],
        extra_options: Mapping[str, Any],
    ) -> DatabaseConnector:
        if connector is not None:
            return connector

        if isinstance(auth_credentials, DatabaseConnector):
            return auth_credentials

        db_path = self._extract_database_path(
            auth_credentials=auth_credentials,
            database_path=database_path,
            extra_options=extra_options,
        )
        if db_path is not None:
            return LocalDuckDBConnector(db_path)

        if isinstance(auth_credentials, Mapping):
            md_name = auth_credentials.get("database_name")
            md_token = auth_credentials.get("token")
            if md_name or md_token:
                return MotherDuckConnector(database_name=md_name, token=md_token)

        md_name = extra_options.get("database_name")
        md_token = extra_options.get("token")
        if md_name or md_token:
            return MotherDuckConnector(database_name=md_name, token=md_token)

        return LocalDuckDBConnector(self._default_database_path(self.env))

    def _extract_database_path(
        self,
        *,
        auth_credentials: Optional[Any],
        database_path: Optional[Union[str, Path]],
        extra_options: Mapping[str, Any],
    ) -> Optional[Path]:
        candidates: Tuple[Any, ...] = (
            database_path,
            extra_options.get("database_path"),
        )

        for candidate in candidates:
            if not candidate:
                continue
            if isinstance(candidate, str) and candidate.startswith("md:"):
                continue
            return Path(candidate)

        if isinstance(auth_credentials, (str, Path)):
            if isinstance(auth_credentials, str) and auth_credentials.startswith("md:"):
                return None
            return Path(auth_credentials)

        if isinstance(auth_credentials, Mapping):
            candidate = auth_credentials.get("database_path")
            if isinstance(candidate, str) and candidate.startswith("md:"):
                return None
            if candidate:
                return Path(candidate)

        return None

    def _default_database_path(self, env: str) -> Path:
        base = Path(duckdb_path)
        if env.lower() != "prod":
            token = sanitize_table_name(env) or env.lower()
            return base.with_name(f"{base.stem}_{token}{base.suffix}")
        return base

    def _resolve_location(self) -> str:
        if hasattr(self.connector, "database_path"):
            return str(getattr(self.connector, "database_path"))
        if hasattr(self.connector, "database_name"):
            return f"md:{getattr(self.connector, 'database_name')}"
        return "duckdb"

    @contextmanager
    def _connection(self) -> Iterator[DuckDBPyConnection]:
        conn = self.connector.connect()
        try:
            if self.schema.lower() != "main":
                conn.execute(
                    f"CREATE SCHEMA IF NOT EXISTS {self._quote_identifier(self.schema)}"
                )
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _quote_identifier(name: str) -> str:
        # Escape embedded quotes by doubling them, per SQL standard
        # Note we have already sanitized the name to avoid quotes; this is just in case.
        safe = name.replace('"', '""')
        return f'"{safe}"'

    def _qualified(self, table_name: str) -> str:
        # Return a schema-qualified table name with proper quoting
        return (
            f"{self._quote_identifier(self.schema)}."
            f"{self._quote_identifier(table_name)}"
        )

    def _table_ref(self, table_name: str) -> TableVersionRef:
        return TableVersionRef(
            table_name,
            env=self.env,
            timestamp=self.timestamp,
            formatter=sanitize_table_name,
        )

    def _iter_dataframes(
        self,
        data: Union[DataFrame, Iterable[DataFrame]],
    ) -> Iterator[DataFrame]:
        if isinstance(data, DataFrame):
            yield data
            return

        try:
            iterator = iter(data)
        except TypeError as exc:
            raise TypeError(
                "`data` must be a DataFrame or an iterable of DataFrames.",
            ) from exc

        for item in iterator:
            if not isinstance(item, DataFrame):
                raise TypeError(
                    "`data` must be a DataFrame or an iterable of DataFrames.",
                )
            yield item

    def _write_dataframe(
        self,
        conn: DuckDBPyConnection,
        df: DataFrame,
        table_name: str,
        *,
        append: bool,
    ) -> None:
        temp_view = f"_df_{uuid.uuid4().hex}"
        conn.register(temp_view, df)
        qualified = self._qualified(table_name)
        try:
            if append:
                conn.execute(f"INSERT INTO {qualified} SELECT * FROM {temp_view}")
            else:
                conn.execute(
                    f"CREATE OR REPLACE TABLE {qualified} AS SELECT * FROM {temp_view}"
                )
        finally:
            conn.unregister(temp_view)

    def _table_exists(self, conn: DuckDBPyConnection, table_name: str) -> bool:
        query = (
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ? LIMIT 1"
        )
        return conn.execute(query, [self.schema, table_name]).fetchone() is not None

    def _read_table(self, conn: DuckDBPyConnection, table_name: str) -> DataFrame:
        return conn.execute(f"SELECT * FROM {self._qualified(table_name)}").df()

    def _prep_table_name(
        self,
        sheet_name: str,
        worksheet_name: Optional[str] = None,
    ) -> str:
        base = sanitize_table_name(sheet_name)
        if not base:
            raise ValueError(
                f"Sheet name '{sheet_name}' collapses to an empty identifier after"
                "sanitization.",
            )

        table = f"{base}{self.env_suffix}"
        if worksheet_name is not None:
            suffix = sanitize_table_name(str(worksheet_name))
            if not suffix:
                raise ValueError(
                    f"Worksheet name '{worksheet_name}' collapses to an empty "
                    "identifier after sanitization.",
                )
            table = f"{table}__{suffix}"
        return table

    def _table_url(self, table_name: str) -> str:
        return f"duckdb://{self._location}#{table_name}"


__all__ = ["DuckDBWarehouseService"]
