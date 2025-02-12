"""stg_results.py.

This script interacts with Google Drive to fetch Google Sheets containing race results,
transforms the data, and inserts it into a DuckDB database.
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
from duckdb import DuckDBPyConnection
from tqdm import tqdm

from mrl_pipeline import google_service_account_credentials
from mrl_pipeline.data_connectors import DriveService

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
    return conn.execute(
        """
        CREATE OR REPLACE TABLE stg_results (
            source_file_id VARCHAR,
            race_id VARCHAR,
            {mapped_columns},
            created_at timestamp DEFAULT current_timestamp
        );
        """.format(
            mapped_columns=[",\n".join(f"{k} VARCHAR" for k in COLUMN_MAP)],
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
        """
        CREATE OR REPLACE TABLE ? AS
            SELECT *
            FROM read_csv(
                ?,
                auto_type_candidates = ['VARCHAR'],
                header = true
            );
        """,
        [temp_table_name, temp_url],
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
    return conn.execute(
        """
        INSERT INTO stg_results BY NAME
            SELECT
                ?,
                ? as source_file_id,
                ? as race_id
            FROM ?
        """,
        [
            mapped_columns_str,
            sheet_id,
            race_id,
            temp_table_name,
        ],
    )


def main() -> None:
    """Main function to fetch race results from Google Sheets and insert them into
    DuckDB.
    """
    # Get Google credentials from environment variable
    drive = DriveService(google_service_account_credentials)
    df_drive_ids = drive.get_gsheets_by_prefix("prep_results")

    # Connect to DuckDB database
    duckdb_con = duckdb.connect("prod.duckdb")

    def write_from_thread(sheet_data: dict) -> None:
        """Writes race results from a single sheet into the database in a separate
        thread.

        Args:
            sheet_data: Dictionary containing Google Sheet metadata.

        """
        local_con = duckdb_con.cursor()
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


if __name__ == "__main__":
    main()
