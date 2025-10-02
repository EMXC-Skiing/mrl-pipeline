"""infra_result_files.py."""

from types import ModuleType
from typing import Union

import duckdb
from dagster_duckdb import DuckDBResource
from duckdb import DuckDBPyConnection

from mrl_pipeline.io.drive_service import DriveService
from mrl_pipeline.models import PipelineModel
from mrl_pipeline.utils import (
    duckdb_path,
    google_service_account_path,
)


def run_infra_result_files(
    duckdb_resource: Union[DuckDBResource, ModuleType],
) -> DuckDBPyConnection:
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

    return conn


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
