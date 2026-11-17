from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Any,
    Mapping,
    Optional,
    Sequence,
)

from pandas import DataFrame


@dataclass(frozen=True)
class TableRef:
    """
    Lightweight, immutable pointer to a table address in a backend.
    """

    logical_name: str  # e.g. "c"
    physical_name: str  # e.g. "__tmp_c__run123" or "c" (canonical)
    schema: Optional[str] = None  # optional default schema
    backend: Optional[str] = None  # e.g. "duckdb", "motherduck", "gsheets"


@dataclass(frozen=True)
class TableSummary:
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    columns: Optional[Sequence[str]] = None
    dtypes: Optional[Mapping[str, str]] = None
    notes: Optional[str] = None


class WarehouseService(ABC):
    """
    Backend-specific implementation: DuckDB, MotherDuck, Sheets, etc.
    """

    @property
    @abstractmethod
    def backend_id(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def set_default_schema(self, schema: str) -> None:
        """Set the schema context so SQL can reference unqualified table names."""
        raise NotImplementedError

    @abstractmethod
    def quote_ident(self, ident: str) -> str:
        """Quote/escape a single identifier safely for this backend."""
        raise NotImplementedError

    @abstractmethod
    def qualify(self, name: str, *, schema: Optional[str] = None) -> str:
        """Return a fully-qualified identifier string appropriate for SQL."""
        raise NotImplementedError

    @abstractmethod
    def read_table(
        self,
        table: TableRef,
        *,
        columns: Optional[Sequence[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def write_table(
        self,
        table: TableRef,
        df: DataFrame,
        *,
        mode: str = "replace",  # "replace" | "append" | "fail"
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute_sql(
        self,
        query: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Execute a statement (DDL/DML) in the backend."""
        raise NotImplementedError

    # --- catalog / lifecycle primitives ---
    @abstractmethod
    def exists(self, table: TableRef) -> bool:
        raise NotImplementedError

    @abstractmethod
    def drop(self, table: TableRef, *, if_exists: bool = True) -> None:
        raise NotImplementedError

    @abstractmethod
    def rename(self, src: TableRef, dst: TableRef, *, replace: bool = False) -> None:
        raise NotImplementedError

    @abstractmethod
    def summarize(self, table: TableRef) -> TableSummary:
        raise NotImplementedError

    @abstractmethod
    def promote_temp_tables(self, tables) -> None:
        """
        Execute a run-level promotion plan (promote temps + archive old canonicals),
        ideally atomically (often inside a single transaction).
        """
        raise NotImplementedError
