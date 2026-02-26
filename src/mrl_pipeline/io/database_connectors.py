# mrl_pipeline/io/database_connectors.py
from __future__ import annotations

import pathlib
from abc import ABC, abstractmethod
from typing import Optional, Union

import duckdb
from duckdb import DuckDBPyConnection

from mrl_pipeline.io.table_version_ref import escape_table_name
from mrl_pipeline.settings import settings


def make_default_connector() -> DatabaseConnector:
    if settings.motherduck_token:
        return MotherDuckConnector(
            database_name=settings.database_name,
            motherduck_token=settings.motherduck_token,
            schema_name=settings.schema_name or None,
        )
    return LocalDuckDBConnector(
        database_path=settings.local_duckdb_path,
        schema_name=settings.schema_name or None,
    )


class DatabaseConnector(ABC):
    @abstractmethod
    def connect(self) -> DuckDBPyConnection:
        raise NotImplementedError


def _try_load_httpfs(conn: DuckDBPyConnection) -> None:
    try:
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
    except Exception:
        # Optionally log instead of swallowing.
        pass


class LocalDuckDBConnector(DatabaseConnector):
    """Connection to a local DuckDB database file (or :memory:)."""

    def __init__(
        self,
        database_path: Optional[Union[str, pathlib.Path]] = None,
        schema_name: str | None = None,
    ) -> None:
        # Use settings default if not provided.
        path = (
            database_path if database_path is not None else settings.local_duckdb_path
        )

        if path is None:
            self.database_path = ":memory:"
        else:
            p = pathlib.Path(path).expanduser()
            # Create parent directory for file-backed DBs
            if str(p) != ":memory:":
                p.parent.mkdir(parents=True, exist_ok=True)
            self.database_path = str(p)

        self.schema_name = (schema_name or settings.schema_name or "").strip() or None

    def connect(self) -> DuckDBPyConnection:
        conn = duckdb.connect(self.database_path)
        _try_load_httpfs(conn)

        if self.schema_name:
            conn.execute(
                f"CREATE SCHEMA IF NOT EXISTS {escape_table_name(self.schema_name)};"
            )
            conn.execute("SET schema = ?", [self.schema_name])
        return conn


class MotherDuckConnector(DatabaseConnector):
    """Connects to a MotherDuck-hosted DuckDB instance."""

    def __init__(
        self,
        database_name: Optional[str] = None,
        motherduck_token: Optional[str] = None,
        schema_name: str | None = None,
    ) -> None:
        self.database_name = database_name or settings.database_name
        self.motherduck_token = motherduck_token or settings.motherduck_token
        self.schema_name = (schema_name or settings.schema_name or "").strip() or None

        if not self.database_name:
            raise RuntimeError("Missing configuration value for database_name.")
        if not self.motherduck_token:
            raise RuntimeError("Missing configuration value for motherduck_token.")

    def connect(self) -> DuckDBPyConnection:
        conn = duckdb.connect(f"md:{self.database_name}")
        conn.execute("SET motherduck_token = ?", [self.motherduck_token])
        _try_load_httpfs(conn)

        if self.schema_name:
            conn.execute(
                f"CREATE SCHEMA IF NOT EXISTS {escape_table_name(self.schema_name)};"
            )
            conn.execute("SET schema = ?", [self.schema_name])

        return conn
