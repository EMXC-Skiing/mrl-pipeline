"""Shared lifecycle table naming helpers."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from mrl_pipeline.io.utils import (
    _default_timestamp,
    escape_table_name,
    sanitize_table_name,
)
from mrl_pipeline.settings import settings


class TableVersionRef:
    """Generate deterministic lifecycle table names for a logical table.

    Usage pattern:
    - Instantiate once per logical write/read operation:
      ``ref = TableVersionRef("results", env="staging")``
    - Use key access for lifecycle phases:
      ``ref["temp"]`` for staged writes,
      ``ref["primary"]`` for the promoted canonical table,
      ``ref["archive"]`` for the previous primary snapshot name.
    - Use ``as_dict()`` when an adapter needs all names at once.

    Naming behavior:
    - ``base`` is derived from ``table_name`` via ``formatter`` (or ``str`` fallback).
    - ``env`` defaults to ``settings.environment_name``.
    - ``env="prod"`` produces no environment suffix; non-prod adds ``_{env}``.
    - ``timestamp`` defaults to UTC via ``_default_timestamp()`` and is used only
      for archive naming.


    Formatter contract:
    - ``formatter`` should return a storage-safe identifier string.
    - ``table_name`` must resolve to a non-empty value; otherwise ``ValueError``.
    """

    def __init__(
        self,
        base_name: str,
        env: Optional[str] = None,
        timestamp: Optional[str] = None,
        formatter: Optional[Callable[[Any], str]] = None,
    ) -> None:
        self.base_name = base_name
        self.env = env or settings.environment_name
        self.timestamp = timestamp
        self.formatter = formatter or sanitize_table_name

        self.original_name = self.base_name

        base = self._format_value(self.base_name, "base_name")
        self.base = base

        env_formatted = self._format_value(self.env, "environment", allow_empty=True)
        env_suffix_token = env_formatted or self.env.lower()
        self.env_suffix = "" if self.env.lower() == "prod" else f"_{env_suffix_token}"

        ts_input = self.timestamp or _default_timestamp()
        ts_formatted = self._format_value(ts_input, "timestamp", allow_empty=True)
        if not ts_formatted:
            ts_formatted = self._format_value(
                _default_timestamp(),
                "timestamp",
                allow_empty=True,
            )
        self.timestamp = ts_formatted

        names = {
            "primary": f"{base}{self.env_suffix}",
            "temp": f"__{base}_TEMP{self.env_suffix}",
            "archive": f"_{base}{self.env_suffix}_{ts_formatted}",
        }
        names = {key: escape_table_name(name) for key, name in names.items()}
        self.names = names

    def __getitem__(self, key: str) -> str:
        try:
            return self.names[key]
        except KeyError as exc:
            valid = ", ".join(sorted(self.names))
            raise KeyError(
                f"Unknown table lifecycle key '{key}'. Expected one of: {valid}."
            ) from exc

    def as_dict(self) -> Dict[str, str]:
        return dict(self.names)

    def _format_value(
        self,
        value: Any,
        label: str,
        *,
        allow_empty: bool = False,
    ) -> str:
        formatted = self.formatter(value) if value is not None else self.formatter("")
        if not isinstance(formatted, str):
            formatted = str(formatted)
        if not formatted and not allow_empty:
            raise ValueError(f"{label} yields an empty value after formatting.")
        return formatted
