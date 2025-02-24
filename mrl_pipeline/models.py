"""models.py.

This module defines the base class from which MRL models are instantiated, as well as
the models themselves.

"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
from dagster import TableColumn, TableSchema, asset
from duckdb import BinderException, DuckDBPyConnection
from tqdm import tqdm

from mrl_pipeline.data_connectors import DriveService
from mrl_pipeline.utils import (
    duckdb_path,
    google_service_account_path,
    sanitize_table_name,
)


class PipelineModel:
    """Base class representing a model within the MRL pipeline.

    This class encapsulates both metadata and execution logic for a model,
    enabling it to be integrated as a dagster asset.

    Attributes:
        name (str): Unique identifier for the model.
        description (str): Brief summary of the model's purpose.
        column_descriptions (dict[str, str] | None): Optional mapping of column names to
            their descriptions.
        deps (list[str]): List of asset names that this model depends on.
        runner (callable): Function or callable that performs the model's computations.

    """

    def __init__(
        self,
        name: str,
        description: str,
        deps: list[str],
        runner: callable,
        column_descriptions: dict[str, str] | None = None,
    ) -> None:
        """Initialize a PipelineModel instance with its metadata and execution
            parameters.

        Args:
            name (str): Unique identifier for the model.
            description (str): Brief summary of the model's purpose.
            column_descriptions (dict[str, str] | None): Optional mapping of column
                names to their descriptions.
            deps (list[str]): List of asset names that this model depends on.
            runner (callable): Function or callable that performs the model's
                computations.


        """
        self.name = name
        self.description = description
        self.deps = deps
        self.runner = runner
        self.column_descriptions = column_descriptions or {}
        self.asset_metadata = {
            "dagster/column_schema": TableSchema(
                columns=[
                    TableColumn(name=k, description=v)
                    for k, v in self.column_descriptions.items()
                ],
            ),
        }

    def run(self, *args, **kwargs):
        """Execute the model's logic with any provided arguments and keyword arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            Any: The result of the model's computation.

        """
        return self.runner(*args, **kwargs)

    def build(self):
        """Create a Dagster asset that wraps the model's execution logic.

        This method uses the Dagster @asset decorator to convert the model's runner
        function into an asset.

        Returns:
            Callable: A Dagster asset that executes the model's logic.

        """

        @asset(
            name=self.name,
            deps=self.deps,
            description=self.description,
            metadata=self.asset_metadata,
        )
        def run_model(context):
            # Run the model with any provided arguments and keyword arguments.
            return self.runner(context)

        return run_model


def run_infra_result_files(context) -> duckdb.DuckDBPyConnection:
    """Main function to add table of Google Drive ids to DuckDB database."""
    # Get Google credentials from environment variable
    drive = DriveService(google_service_account_path)

    # Get Google Drive ids for all Google Sheets with the prefix "prep_results"
    df_drive_ids = drive.get_gsheets_by_prefix("prep_results")  # noqa: F841

    # Connect to DuckDB database
    conn = duckdb.connect(duckdb_path)

    # Add the Google Drive IDs to the DuckDB database
    conn.execute("""
        CREATE OR REPLACE TABLE infra_result_files (
                race_id VARCHAR,
                file_id VARCHAR,
                modified_at TIMESTAMP,
                source_type VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO infra_result_files SELECT
                race_id,
                file_id,
                modified_at,
                source_type
            FROM df_drive_ids
    """)

    return


infra_result_files = PipelineModel(
    name="infra_result_files",
    description="File identifiers of prepped result sheets by race id.",
    deps=[],
    column_descriptions={
        "race_id": "Unique race identifier",
        "file_id": "File identifier",
        "modified_at": "File last modified timestamp",
        "source_type": "Type of file source",
    },
    runner=run_infra_result_files,
)


def run_stg_results(context) -> DuckDBPyConnection:
    # Mapping of Google Sheets column names to staging table column names
    column_map = {
        "name_raw": ["Name", "Skier Name"],
        "time_raw": ["Race Time", "Elapsed"],
        "usss_id": ["USSS#", "Member #"],
        "fis_id": ["FIS#"],
        "school_raw": ["School"],
        "team_raw": ["Team"],
        "state_raw": ["State"],
    }

    def create_or_replace_staging_table(conn: DuckDBPyConnection) -> DuckDBPyConnection:
        query = """
                CREATE OR REPLACE TABLE stg_results (
                    file_id VARCHAR,
                    race_id VARCHAR,
                    {},
                    created_at timestamp DEFAULT current_timestamp
                );
                """
        return conn.execute(
            query.format(
                ",\n".join(f"{k} VARCHAR" for k in column_map),
            ),
        )

    def transform_results_and_insert_into_staging_table(
        conn: DuckDBPyConnection,
        sheet_data_dict: dict,
    ) -> DuckDBPyConnection:
        sheet_id = sheet_data_dict["file_id"]
        race_id = sheet_data_dict["race_id"]
        temp_table_name = sanitize_table_name(sheet_data_dict["race_id"])
        temp_url = (
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv"
        )

        # Create a temporary table from the Google Sheet CSV export
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE "{temp_table_name}" AS
                SELECT *
                FROM read_csv(
                    ?,
                    auto_type_candidates = ['VARCHAR'],
                    header = true
                );
            """,  # noqa: S608
            [temp_url],
        )

        # Retrieve column names from the temporary table
        column_names = (
            conn.execute(f"PRAGMA table_info('{temp_table_name}')")
            .df()["name"]
            .tolist()
        )

        # Map column names using column_map, or null
        mapped_columns = {
            key: next((f'"{col}"' for col in values if col in column_names), "null")
            for key, values in column_map.items()
        }

        # Construct select statement for inserting mapped columns
        mapped_columns_str = ",\n\t".join(
            [
                f"{old_col_name} AS {new_col_name}"
                for new_col_name, old_col_name in mapped_columns.items()
            ],
        )

        # Insert transformed data into the staging table
        try:
            return conn.execute(
                f"""
                INSERT INTO stg_results BY NAME
                    SELECT
                        {mapped_columns_str},
                        ? as file_id,
                        ? as race_id
                    FROM "{temp_table_name}";
                """,  # noqa: S608
                [
                    sheet_id,
                    race_id,
                ],
            )
        except BinderException as e:
            print(
                f"Error inserting data from race {race_id} and sheet {sheet_id}:"
                f" {mapped_columns_str}",
            )
            print(e)
            return None

    """Main function to fetch race results from Google Sheets and insert them into
    DuckDB.
    """
    # Connect to DuckDB database
    conn = duckdb.connect(duckdb_path)

    # install httpfs extension
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    # Create or replace staging table
    create_or_replace_staging_table(conn)

    # function to write table data from Python thread
    def write_from_thread(sheet_data: dict) -> None:
        """Writes race results from a single sheet into the database in a separate
        thread.

        Args:
            sheet_data: Dictionary containing Google Sheet metadata.

        """
        local_con = conn.cursor()
        return transform_results_and_insert_into_staging_table(local_con, sheet_data)

    # Get infra_result_files
    df_drive_ids = conn.execute("SELECT * FROM infra_result_files").df()

    with ThreadPoolExecutor() as executor:
        # Create writer tasks
        futures = []
        for _, row in df_drive_ids.iterrows():
            future = executor.submit(write_from_thread, row.to_dict())
            futures.append(future)

        # Initialize progress bar
        with tqdm(total=len(df_drive_ids), desc="Processing", unit="task") as pbar:
            for future in as_completed(futures):
                future.result()  # Ensure task completion
                pbar.update(1)  # Update progress bar

    return


stg_results = PipelineModel(
    name="stg_results",
    description="Staging table for race results, aggregated across individual races.",
    deps=["infra_result_files"],
    column_descriptions={
        "race_id": "Unique race identifier",
        "file_id": "Identifier of Google Drive source file",
        "name_raw": "Athlete name as it appears in the source file",
        "time_raw": "Race time as it appears in the source file",
        "usss_id": "Athlete's USSS number as it appears in the source file",
        "fis_id": "Athlete's FIS number as it appears in the source file",
        "school_raw": "Athlete's school as it appears in the source file",
        "team_raw": "Athlete's team as it appears in the source file",
        "state_raw": "Athlete's state as it appears in the source file",
    },
    runner=run_stg_results,
)
