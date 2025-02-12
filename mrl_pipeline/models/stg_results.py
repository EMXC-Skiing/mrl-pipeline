"""stg_results.py.

This script interacts with Google Drive to fetch Google Sheets containing race results,
transforms the data, and inserts it into a DuckDB database.
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
from duckdb import BinderException, DuckDBPyConnection
from tqdm import tqdm

from mrl_pipeline.data_connectors import DriveService
from mrl_pipeline.models import PipelineModel
from mrl_pipeline.utils import duckdb_path, google_service_account_path

# Mapping of Google Sheets column names to staging table column names
COLUMN_MAP = {
    "name_raw": ["Name", "Skier Name"],
    "time_raw": ["Race Time", "Elapsed"],
    "usss_id": ["USSS#", "Member #"],
    "fis_id": ["FIS#"],
    "school_raw": ["School"],
    "team_raw": ["Team"],
    "state_raw": ["State"],
}


def create_or_replace_staging_table(conn: DuckDBPyConnection) -> DuckDBPyConnection:
    """Creates or replaces the staging table for storing race results."""
    query = """
            CREATE OR REPLACE TABLE stg_results (
                source_file_id VARCHAR,
                race_id VARCHAR,
                {},
                created_at timestamp DEFAULT current_timestamp
            );
            """
    return conn.execute(
        query.format(
            ",\n".join(f"{k} VARCHAR" for k in COLUMN_MAP),
        ),
    )


def sanitize_table_name(name: str) -> str:
    """Sanitizes and validates a table name, allowing only alphanumeric characters,
    underscores, and plus signs.

    Args:
        name (str): The original table name.

    Returns:
        str: The sanitized table name.

    """
    return re.sub(r"[^a-zA-Z0-9_+]", "", name)  # Remove invalid characters


def transform_results_and_insert_into_staging_table(
    conn: DuckDBPyConnection,
    sheet_data_dict: dict,
) -> DuckDBPyConnection:
    """Transforms race results from a Google Sheet and inserts them into the staging
    table.

    Args:
        conn: The DuckDB connection object.
        sheet_data_dict (dict): Dictionary containing metadata and sheet data.

    Returns:
        The result of the DuckDB insert operation.

    """
    sheet_id = sheet_data_dict["source_file_id"]
    race_id = sheet_data_dict["race_id"]
    temp_table_name = sanitize_table_name(sheet_data_dict["race_id"])
    temp_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv"

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
        conn.execute(f"PRAGMA table_info('{temp_table_name}')").df()["name"].tolist()
    )

    # Map column names using COLUMN_MAP, or null
    mapped_columns = {
        key: next((f'"{col}"' for col in values if col in column_names), "null")
        for key, values in COLUMN_MAP.items()
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
                    ? as source_file_id,
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


def main(db_path: str) -> None:
    """Main function to fetch race results from Google Sheets and insert them into
    DuckDB.
    """
    # Get Google credentials from environment variable
    drive = DriveService(google_service_account_path)
    df_drive_ids = drive.get_gsheets_by_prefix("prep_results")

    # Connect to DuckDB database
    conn = duckdb.connect(db_path)

    # install httpfs extension
    conn.execute("""
        INSTALL httpfs; LOAD httpfs;
    """)

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


stg_results = PipelineModel

if __name__ == "__main__":
    conn = main(db_path=duckdb_path)
    print(conn.execute("select count(*) from stg_results").df())
