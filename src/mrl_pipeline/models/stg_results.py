"""models.py.

This module defines the base class from which MRL models are instantiated.

"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from types import ModuleType
from typing import Union

from dagster_duckdb import DuckDBResource
from duckdb import BinderException, DuckDBPyConnection
from tqdm import tqdm

from mrl_pipeline.models import PipelineModel
from mrl_pipeline.utils import (
    duckdb_path,
    sanitize_table_name,
)


def run_stg_results(
    duckdb_resource: Union[DuckDBResource, ModuleType],
) -> DuckDBPyConnection:
    """Connects to a DuckDB database, creates or replaces a staging table,
    and fetches race results from Google Sheets. The results are then transformed and
    inserted into the staging table.

    Returns:
        DuckDBPyConnection: The DuckDB connection object.

    """
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
    conn = duckdb_resource.connect(duckdb_path)

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

    return conn


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
