from datetime import datetime

import pytz
from gspread.exceptions import WorksheetNotFound

from mrl_pipeline import CONFIG_PATH
from mrl_pipeline.io.resilient_gspread_client import ResilientGspreadClient
from mrl_pipeline.io.schema_gsheet import SchemaGSheet


def _return_timestamp():
    eastern = pytz.timezone("America/New_York")
    return datetime.now(eastern).strftime("%y-%m-%d_%H:%M")


class TableVersionRef:
    def __init__(self, table_name, env="prod", timestamp=None):
        """:param table_name: The base name of the table.
        :param env: The target environment.
        :param timestamp: Optional timestamp for versioning archived tables.
        """
        self.table_name = table_name
        self.env = env
        self.tstamp = timestamp or _return_timestamp()
        self.env_suffix = "" if self.env == "prod" else f"_{env}"

        self._dict = {
            "primary": f"{self.table_name}{self.env_suffix}",
            "temp": f"__{self.table_name}_TEMP{self.env_suffix}",
            "archive": f"_{self.table_name}{self.env_suffix}_{self.tstamp}",
        }

    def __getitem__(self, key):
        return self._dict.get(key, None)


class TableVersionManager:
    def __init__(self, gclient, env_folder_ids, env="prod", timestamp=None):
        """Manages lifecycle versions of tables, with environment-based folder and name
        management.

        :param gclient: The Google Sheets client.
        :param env_folder_ids: A dictionary mapping environments to folder IDs.
        :param env: Target environment, defaulting to 'prod'.
        :param timestamp: Optional timestamp for archiving.
        """
        self.gc = gclient
        self.env_folder_ids = env_folder_ids
        self.env = env
        self.timestamp = timestamp or _return_timestamp()
        self.temp_tables = {}

    def create_table(self, table_name, lifecycle_management=True):
        """Creates a new Google Sheet in the appropriate environment folder
        and returns the Google Sheets object for that worksheet.
        Adds the new sheet to the temporary tables dict to be promoted later.

        :param table_name: The base name of the table to create.
        :return: The Google Sheet object created for the temp version.
        """
        # Create TableVersionRef to manage naming
        table_ref = TableVersionRef(table_name, env=self.env, timestamp=self.timestamp)

        # Identify the correct folder ID based on the environment
        folder_id = self.env_folder_ids.get(self.env)
        if not folder_id:
            raise ValueError(f"No folder ID specified for environment '{self.env}'.")

        if lifecycle_management:
            # Create the temporary sheet and store its reference
            sh = self.gc.create(table_ref["temp"], folder_id=folder_id)
            self.temp_tables[table_name] = (table_ref, sh.url)
        else:
            sh = self.gc.create(table_ref["primary"], folder_id=folder_id)

        return sh

    def retrieve_latest(self, table_name):
        """Retrieves the Google Sheets object for a table, preferring the temporary
        version if it exists in `temp_tables`, otherwise returning the production
        version.

        :param table_name: The base name of the table to retrieve.
        :return: A gspread sheet object for the temporary or production table.
        """
        # Generate the table reference with environment-based naming
        table_ref = TableVersionRef(table_name, env=self.env, timestamp=self.timestamp)

        # Check for a temporary table by matching its name in temp_tables
        if table_name in self.temp_tables.keys():
            return self.gc.open(table_ref["temp"])

        # If no temporary table found, retrieve the production version
        try:
            return self.gc.open(table_ref["primary"])
        except gspread.SpreadsheetNotFound:
            raise ValueError(
                f"No table found for '{table_name}' in the {self.env} environment.",
            )

    def promote_temp_tables(self, archive_previous=True):
        """Promotes all temporary tables to production, optionally archiving existing
        production versions.

        :param archive_previous: If True, archives the previous primary version instead
        of deleting it.
        """
        while self.temp_tables:
            table_name, (table_ref, sheet_url) = self.temp_tables.popitem()

            try:
                # Open the previous primary sheet
                previous_sheet = self.gc.open(table_ref["primary"])

            except gspread.SpreadsheetNotFound:
                # Production sheet doesn't exist; create one!
                previous_sheet = None

            # Rename the temp sheet to the production name
            new_sheet = self.gc.open_by_url(sheet_url)
            new_sheet.update_title(table_ref["primary"])

            # handle previous primary sheet if it exists
            if previous_sheet is not None:
                if archive_previous:
                    # Archive the previous primary sheet
                    previous_sheet.update_title(table_ref["archive"])
                else:
                    # Delete the primary sheet
                    self.gc.del_spreadsheet(previous_sheet.id)


import json

import gspread
from google.oauth2.service_account import Credentials
from pandas import DataFrame


class SheetService:
    def __init__(
        self,
        auth_credentials: str,
        env: str = "prod",
        warehouse_folder_ids: str = None,
        **kwargs,
    ):
        """Initializes the high-level data interface, setting up the environment
        and client and initializing necessary managers.

        :param client: The base authenticated gspread client
        :param env: Environment, e.g., 'prod' or 'dev', to control behavior like sheet
        naming.
        :param kwargs: Additional arguments passed for specific configuration,
                       e.g., folder_id, retry parameters, etc.
        """
        self.env = env

        if warehouse_folder_ids is None:
            with open(CONFIG_PATH / "drive_environments.json", "r") as f:
                self.warehouse_folder_ids = json.load(f)
        else:
            self.warehouse_folder_ids = warehouse_folder_ids

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        creds = Credentials.from_service_account_info(
            json.loads(auth_credentials),
        ).with_scopes(scope)
        base_gc = gspread.authorize(creds)
        self.gc = ResilientGspreadClient(base_gc)
        self.version_manager = TableVersionManager(
            self.gc, env_folder_ids=self.warehouse_folder_ids, env=self.env, **kwargs,
        )

    def put_warehouse_table(self, table_name: str, data, lifecycle_management=True):
        """Writes data to a warehouse table, handling either a single DataFrame or an
        iterable of DataFrames.

        :param table_name: The name of the table to write.
        :param data: A DataFrame or an iterable of DataFrames containing data to be
        written.
        """
        # Create a temporary sheet for the provided table name
        sh = self.version_manager.create_table(
            table_name, lifecycle_management=lifecycle_management,
        )

        try:
            # Try treating `data` as a single DataFrame
            with SchemaGSheet(sh, "w") as schema_sheet:
                schema_sheet.write_dataframe(data)
        except (AttributeError, TypeError):
            # If `data` isn't a DataFrame, attempt to iterate over it
            try:
                with SchemaGSheet(sh, "w") as schema_sheet:
                    for df in data:
                        schema_sheet.write_dataframe(df)
            except TypeError:
                raise TypeError(
                    "`data` must be a DataFrame or an iterable of DataFrames.",
                )

    def get_warehouse_table(self, table_name: str, return_url: bool = False):
        """Loads data from a specified table into a DataFrame, retrieving
        the latest version of the table from the table version manager.

        :param table_name: The base name of the table to load data from.
        :return: A DataFrame containing the data loaded from the specified Google Sheets
        table.
        """
        try:
            sh = self.version_manager.retrieve_latest(table_name)
        except gspread.SpreadsheetNotFound:
            print(f"Spreadsheet not found: {table_name}")
            raise
        with SchemaGSheet(sh, "r") as schema_sheet:
            df = schema_sheet.read_dataframe()

        if return_url:
            return df, sh.url
        return df

    def promote_temp_tables(self, archive_previous: bool = True):
        """Promotes all temporary tables to their production equivalents,
        archiving the previous versions by default.

        :param archive_previous: If True, archives the previous version before
            promotion.
        """
        self.version_manager.promote_temp_tables(archive_previous=archive_previous)

    def get_prep_table(
        self, sheet_name, worksheet_name: str = None, return_url: bool = False,
    ):
        """Retrieves a worksheet from a specified sheet as a DataFrame. If
        `worksheet_name` is provided, get_warehouse_table will try to interpret it as an
        integer index or string name.

        :param sheet_name: The name of the Google Sheet.
        :param worksheet_name: Optional; an integer for worksheet index or a string for
            worksheet name.
        :return: A DataFrame containing data from the specified worksheet.
        """
        try:
            sh = self.gc.open(sheet_name)
        except gspread.SpreadsheetNotFound:
            print(f"Spreadsheet not found: {sheet_name}", flush=True)
            raise
        try:
            if worksheet_name is None:
                worksheet = sh.get_worksheet(0)
            else:
                worksheet = sh.get_worksheet(int(worksheet_name))
        except (WorksheetNotFound, ValueError):
            # WorksheetNotFound if the worksheet name/index doesn't exist ValueError if
            # conversion to int fails (meaning `worksheet_name` is likely a string)
            worksheet = sh.worksheet(worksheet_name)

        df = DataFrame(worksheet.get_all_records()).convert_dtypes()

        if return_url:
            return df, sh.url
        return df

    def iter_prep_tables(self, sheet_name):
        """Yields DataFrames for each worksheet in the specified sheet, along with their
        titles.

        :param sheet_name: The name of the Google Sheet.
        :yield: Tuple of worksheet title and corresponding DataFrame.
        """
        sh = self.gc.open(sheet_name)
        for worksheet in sh.worksheets():
            df = DataFrame(worksheet.get_all_records()).convert_dtypes()
            yield (worksheet.title, df)
