"""Abstract interface and shared helpers for warehouse backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union

from pandas import DataFrame


class WarehouseService(ABC):
    """Declare a minimal contract for services that expose warehouse-like tables."""

    def __init__(
        self,
        *,
        env_name: str,
        auth_credentials: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        self.auth_credentials = auth_credentials
        self.extra_options: Dict[str, Any] = dict(kwargs)
        self.env_name = env_name

    @abstractmethod
    def put_warehouse_table(
        self,
        table_name: str,
        data: Union[DataFrame, Iterable[DataFrame]],
        *,
        lifecycle_management: bool = True,
    ) -> None:
        """Write one or more DataFrames to the warehouse under ``table_name``."""

    @abstractmethod
    def get_warehouse_table(
        self,
        table_name: str,
        *,
        return_url: bool = False,
    ) -> Union[DataFrame, Tuple[DataFrame, str]]:
        """Read the most recent warehouse snapshot for ``table_name``."""

    @abstractmethod
    def promote_temp_tables(self, *, archive_previous: bool = True) -> None:
        """Finalize staged tables, optionally archiving previous primaries."""

    @abstractmethod
    def get_prep_table(
        self,
        sheet_name: str,
        *,
        worksheet_name: Optional[str] = None,
        return_url: bool = False,
    ) -> Union[DataFrame, Tuple[DataFrame, str]]:
        """Load a single prep worksheet by index or name."""

    @abstractmethod
    def iter_prep_tables(
        self,
        sheet_name: str,
    ) -> Iterable[Tuple[str, DataFrame]]:
        """Yield every worksheet within ``sheet_name`` as ``(title, DataFrame)``."""


def _default_timestamp() -> str:
    # Single source of truth: UTC, sortable-friendly format
    return datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")


class TableVersionRef:
    """Generate lifecycle-aware table names shared across warehouse backends."""

    def __init__(
        self,
        table_name: str,
        env: str = "prod",
        timestamp: Optional[str] = None,
        *,
        formatter: Optional[Callable[[str], str]] = None,
    ) -> None:
        self.original_name = table_name
        self.env = env or "prod"
        self._formatter = formatter or (
            lambda value: value if isinstance(value, str) else str(value)
        )

        self.base = self._format_value(table_name, "table_name")

        env_formatted = self._format_value(self.env, "environment", allow_empty=True)
        if self.env.lower() == "prod":
            self.env_suffix = ""
        else:
            suffix = env_formatted or self.env.lower()
            self.env_suffix = f"_{suffix}"

        ts_input = timestamp or _default_timestamp()
        ts_formatted = self._format_value(ts_input, "timestamp", allow_empty=True)
        if not ts_formatted:
            ts_formatted = self._format_value(
                _default_timestamp(), "timestamp", allow_empty=True
            )
        self.timestamp = ts_formatted

        self._names: Dict[str, str] = {
            "primary": f"{self.base}{self.env_suffix}",
            "temp": f"__{self.base}_TEMP{self.env_suffix}",
            "archive": f"_{self.base}{self.env_suffix}_{self.timestamp}",
        }

    def __getitem__(self, key: str) -> Optional[str]:
        return self._names.get(key)

    def as_dict(self) -> Dict[str, str]:
        return dict(self._names)

    def _format_value(
        self,
        value: Any,
        label: str,
        *,
        allow_empty: bool = False,
    ) -> str:
        formatted = self._formatter(value) if value is not None else self._formatter("")
        if not isinstance(formatted, str):
            formatted = str(formatted)
        if not formatted and not allow_empty:
            raise ValueError(f"{label} yields an empty value after formatting.")
        return formatted


__all__ = ["WarehouseService", "TableVersionRef", "_default_timestamp"]
