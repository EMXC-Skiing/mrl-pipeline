"""Database connector abstractions for DuckDB and MotherDuck."""

from __future__ import annotations

import pathlib
from abc import ABC, abstractmethod
from typing import Optional, Union

import duckdb
from duckdb import DuckDBPyConnection

from mrl_pipeline.utils import duckdb_path, fetch_environ


class DatabaseConnector(ABC):
    """Minimal interface for obtaining a DuckDB connection."""

    @abstractmethod
    def connect(self) -> DuckDBPyConnection:
        """Return a new DuckDB connection."""
        raise NotImplementedError


class LocalDuckDBConnector(DatabaseConnector):
    """Connects to a local DuckDB database file."""

    def __init__(
        self, database_path: Optional[Union[str, pathlib.Path]] = None
    ) -> None:
        if database_path is None:
            database_path = fetch_environ("DUCKDB_PATH")
        self.database_path = str(database_path)

    def connect(self) -> DuckDBPyConnection:
        conn = duckdb.connect(self.database_path)
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        return conn


class MotherDuckConnector(DatabaseConnector):
    """Connects to a MotherDuck-hosted DuckDB instance."""

    def __init__(
        self,
        database_name: Optional[str] = None,
        token: Optional[str] = None,
    ) -> None:
        self.database_name = (
            database_name
            if database_name is not None
            else fetch_environ("MOTHERDUCK_DATABASE")
        )
        self.token = token if token is not None else fetch_environ("MOTHERDUCK_TOKEN")

    def connect(self) -> DuckDBPyConnection:  # noqa: D401
        """Connect to MotherDuck using the configured credentials."""
        conn = duckdb.connect(f"md:{self.database_name}")
        conn.execute("SET motherduck_token = ?", [self.token])
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        return conn


__all__ = [
    "DatabaseConnector",
    "LocalDuckDBConnector",
    "MotherDuckConnector",
]
