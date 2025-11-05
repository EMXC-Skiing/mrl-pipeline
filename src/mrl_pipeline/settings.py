from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_DUCKDB_PATH = PROJECT_ROOT / "db" / "mrl.duckdb"


class Settings(BaseSettings):
    duckdb_path: Path = Field(
        default=DEFAULT_DUCKDB_PATH,
        description="Filesystem location for the local DuckDB database file.",
    )
    data_dir: Path = Field(
        default=DEFAULT_DATA_DIR,
        description="Directory used for reading and writing data artifacts.",
    )
    motherduck_database: str | None = Field(
        default=None,
        description="Name of the MotherDuck database to connect to.",
    )
    motherduck_token: str | None = Field(
        default=None,
        description="Authentication token for MotherDuck connections.",
    )
    google_service_account_credentials: str | Path | dict[str, Any] | None = Field(
        default=None,
        description="JSON payload, local file path, or dict of Google credentials.",
    )
    mnp_google_drive_warehouse_folder_ids: dict[str, str] | None = Field(
        default=None,
        description="Mapping of environment name to Google Drive warehouse folder IDs.",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    @field_validator("mnp_google_drive_warehouse_folder_ids", mode="before")
    @classmethod
    def _parse_folder_ids(cls, value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError as exc:  # pragma: no cover - defensive
                raise ValueError(
                    "Expected JSON mapping for MNP_GOOGLE_DRIVE_WAREHOUSE_FOLDER_IDS",
                ) from exc
        return value


settings = Settings()
