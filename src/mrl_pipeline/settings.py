from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PROJECT_DUCKDB_PATH = PROJECT_ROOT / "db" / "mrl.duckdb"
SECRETS_DIR = PROJECT_ROOT / "config" / "secrets"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        env_prefix="MRL_",
        extra="ignore",
        case_sensitive=False,
        secrets_dir=str(SECRETS_DIR),
    )

    ### Environment config

    environment_name: str = Field(
        description="Name of deployment environment (prod, dev, stg, etc.)"
    )

    ### Google-Drive-specific config

    google_drive_warehouse_folder_id: str | None = Field(
        default=None,
        description="Google Drive folder id containing warehouse data.",
    )

    drive_table_suffix: str = Field(
        default="", description="Suffix attached to table names in Google Drive."
    )

    ### DuckDB-specific config

    local_duckdb_path: Path | None = Field(
        default=PROJECT_DUCKDB_PATH,
        description="Filesystem location for the local DuckDB database file.",
    )

    database_name: str = Field(
        default="mrl", description="Name of DuckDB database name."
    )

    schema_name: str = Field(default="", description="Schema name in DuckDB.")

    db_table_suffix: str = Field(
        default="", description="Suffix attached to table names in DuckDB."
    )

    ### Database and storage secrets

    motherduck_token: str | None = Field(
        default=None,
        description="Authentication token for MotherDuck connection.",
    )

    google_service_account_credentials: dict[str, Any] | None = Field(
        default=None,
        description="Dictionary of Google service account credentials.",
    )


settings = Settings()
