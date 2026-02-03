"""Pipeline model that loads Google Drive metadata into DuckDB."""

from duckdb import DuckDBPyConnection

from mrl_pipeline.io.database_connectors import DatabaseConnector
from mrl_pipeline.io.drive_service import DriveService
from mrl_pipeline.models.pipeline_model import PipelineModel


def run_infra_result_files(
    connector: DatabaseConnector,
) -> DuckDBPyConnection:
    """Main function to add table of Google Drive ids to DuckDB database."""
    drive = DriveService()

    df_drive_ids = drive.get_gsheets_by_prefix("prep_results")  # noqa: F841

    conn = connector.connect()
    conn.register("drive_ids", df_drive_ids)

    conn.execute(
        """
        CREATE OR REPLACE TABLE infra_result_files (
                race_id VARCHAR,
                file_id VARCHAR,
                modified_at TIMESTAMP,
                source_type VARCHAR
        )
        """
    )

    conn.execute(
        """
        INSERT INTO infra_result_files SELECT
                race_id,
                file_id,
                modified_at,
                source_type
            FROM drive_ids
        """,
    )

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
