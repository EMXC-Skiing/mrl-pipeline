from gspread import Spreadsheet
from gspread.exceptions import WorksheetNotFound
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from pandas import DataFrame

"""
SchemaGSheet is a class for managing a Google Spreadsheet as a structured table with
schema metadata. It supports both reading and writing modes to allow for controlled
access to the spreadsheet's data. The class operates with two worksheets: one for the
main data table and another for the schema, storing column names, data types, and
optional descriptions.

Attributes:
    sh (Spreadsheet): The Google Spreadsheet object. mode (str): Specifies 'r' for read
    mode or 'w' for write mode. column_descriptions (dict): Optional descriptions for
    each column in the data table. data_sheet_name (str): The name of the data
    worksheet. data_sheet (Worksheet): The worksheet where data is stored.
    schema_sheet_name (str): The name of the schema worksheet. schema_sheet (Worksheet):
    The worksheet where schema metadata is stored. df_schema (DataFrame): DataFrame
    containing schema metadata.

Methods:
    __enter__: Sets up context for 'with' statement use, clearing sheets in write mode.
    __exit__: Handles cleanup actions upon context exit. write_dataframe(df): Writes a
    DataFrame to the data worksheet and initializes schema if absent. read_dataframe:
    Reads data from the data worksheet and applies schema-defined column types.
"""


class SchemaGSheet:
    def __init__(self, sh: Spreadsheet, mode: str, column_descriptions: dict = None):
        """Initializes SchemaGSheet with a Google Spreadsheet instance, mode (read or
        write), and optional column descriptions for schema metadata.
        """
        self.sh = sh
        if mode not in ["r", "w"]:
            raise ValueError("Mode must be 'r' (read) or 'w' (write)")
        self.mode = mode
        self.column_descriptions = column_descriptions or {}

        # reference table data sheet
        self.data_sheet_name = "table_data"
        try:
            self.sh.get_worksheet(0).update_title(self.data_sheet_name)
            self.data_sheet = self.sh.worksheet(self.data_sheet_name)
        except WorksheetNotFound:
            self.data_sheet = self.sh.add_worksheet(
                self.data_sheet_name, rows=10000, cols=20,
            )

        # reference schema sheet
        self.schema_sheet_name = "schema"
        try:
            self.schema_sheet = self.sh.worksheet(self.schema_sheet_name)
        except WorksheetNotFound:
            self.schema_sheet = self.sh.add_worksheet(
                self.schema_sheet_name, rows=100, cols=20,
            )

        # initialize schema
        self.df_schema = None

    def __enter__(self):
        """Sets up the context for using the SchemaGSheet in a 'with' statement,
        including clearing sheets in write mode.
        """
        self.start_row = 1
        self.write_header = True

        # clear sheet if opening it in write mode
        if self.mode == "w":
            self.data_sheet.clear()
            self.schema_sheet.clear()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Handles any cleanup when exiting the context, e.g., closing resources or
        handling exceptions.
        """

    def write_dataframe(self, df: DataFrame):
        """Writes a DataFrame to the data worksheet in Google Sheets. If not already
        defined, initializes schema metadata in the schema sheet based on DataFrame
        columns and types.
        """
        if self.mode != "w":
            raise PermissionError("Sheet is not opened in write mode.")

        # exit if dataframe is empty
        if df.empty:
            return

        # Add rows to sheet if needed, then refresh connection
        overflow_row_ct = (
            df.shape[0]
            + int(self.write_header)
            + self.start_row
            - self.data_sheet.row_count
        )
        if overflow_row_ct > 0:
            # add enough rows to fit
            self.data_sheet.add_rows(overflow_row_ct)
            # reset connection to sheet
            self.data_sheet = self.sh.worksheet(self.data_sheet_name)

        # Add columns to sheet if needed, then refresh connection
        overflow_col_ct = df.shape[1] - self.data_sheet.col_count
        if overflow_col_ct > 0:
            # add enough columns to fit
            self.data_sheet.add_cols(overflow_col_ct)
            # reset connection to sheet
            self.data_sheet = self.sh.worksheet(self.data_sheet_name)

        # write dataframe to worksheet starting on start_row
        set_with_dataframe(
            self.data_sheet,
            dataframe=df,
            row=self.start_row,
            include_column_header=self.write_header,
        )

        # Create and write metadata dataframe if it does not already exist
        if self.df_schema is None:
            self.df_schema = DataFrame(
                {
                    "name": df.columns,
                    "dtype": [str(dtype) for dtype in df.dtypes],
                    "description": [
                        self.column_descriptions.get(c, "") for c in df.columns
                    ],
                },
            )
            set_with_dataframe(self.schema_sheet, self.df_schema)

        # update start_row and write_header
        self.start_row += len(df) + int(self.write_header)
        self.write_header = False

    def read_dataframe(self) -> DataFrame:
        """Reads data from the data worksheet and applies schema from the schema sheet,
        casting columns to their respective types.
        """
        if self.mode != "r":
            raise PermissionError("Sheet is not opened in read mode.")

        # Read data from sheet 1
        raw_data = (
            get_as_dataframe(self.data_sheet, na_values=[""])
            .dropna(axis=0, how="all")
            .dropna(axis=1, how="all")
        )

        # Read metadata from sheet 2 to retrieve column types
        df_schema = (
            get_as_dataframe(self.schema_sheet, na_values=[""], dtype=str)
            .dropna(axis=0, how="all")
            .dropna(axis=1, how="all")
        )

        # Apply the correct dtypes to the raw data
        dtype_dict = {row["name"]: row["dtype"] for _, row in df_schema.iterrows()}
        for column, dtype in dtype_dict.items():
            try:
                raw_data[column] = raw_data[column].astype(dtype)
            except ValueError:
                print(f"Warning: Could not convert column '{column}' to type '{dtype}'")

        return raw_data
