"""Shared lifecycle table naming helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from mrl_pipeline.settings import settings


def _default_timestamp() -> str:
    # Single source of truth: UTC, sortable-friendly format
    return datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")


def sanitize_table_name(name: str) -> str:
    """Normalize raw table text into a predictable identifier token.

    Removes characters outside ``[a-zA-Z0-9_+]`` and lowercases the result.
    This is intended for canonical internal table-name generation, not for SQL
    escaping.
    """
    stripped = re.sub(r"[^a-zA-Z0-9_+]", "", str(name))
    return stripped.lower()


def escape_table_name(name: str) -> str:
    """Return a DuckDB-safe quoted SQL identifier.

    Escapes embedded double quotes by doubling them and wraps the value in
    double quotes. This preserves identifier content while making it safe to
    interpolate into SQL identifier positions.
    """
    safe = str(name).replace('"', '""')
    return f'"{safe}"'


@dataclass(frozen=True)
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
    - ``qualified=True`` applies SQL identifier qualification to all generated
      names using ``schema_name`` (or ``settings.schema_name`` when omitted).

    Formatter contract:
    - ``formatter`` should return a storage-safe identifier string.
    - ``table_name`` must resolve to a non-empty value; otherwise ``ValueError``.
    """

    table_name: str
    env: str = field(default_factory=lambda: settings.environment_name)
    timestamp: Optional[str] = None
    formatter: Optional[Callable[[Any], str]] = None
    qualified: bool = False
    schema_name: Optional[str] = None
    original_name: str = field(init=False)
    base: str = field(init=False)
    env_suffix: str = field(init=False)
    names: Dict[str, str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        formatter = self.formatter or (
            lambda value: value if isinstance(value, str) else str(value)
        )
        env = self.env or settings.environment_name
        object.__setattr__(self, "original_name", self.table_name)
        object.__setattr__(self, "env", env)

        base = self._format_value(self.table_name, "table_name", formatter=formatter)
        object.__setattr__(self, "base", base)

        env_formatted = self._format_value(
            env, "environment", formatter=formatter, allow_empty=True
        )
        env_suffix = "" if env.lower() == "prod" else f"_{env_formatted or env.lower()}"
        object.__setattr__(self, "env_suffix", env_suffix)

        ts_input = self.timestamp or _default_timestamp()
        ts_formatted = self._format_value(
            ts_input, "timestamp", formatter=formatter, allow_empty=True
        )
        if not ts_formatted:
            ts_formatted = self._format_value(
                _default_timestamp(),
                "timestamp",
                formatter=formatter,
                allow_empty=True,
            )
        object.__setattr__(self, "timestamp", ts_formatted)

        names = {
            "primary": f"{base}{env_suffix}",
            "temp": f"__{base}_TEMP{env_suffix}",
            "archive": f"_{base}{env_suffix}_{ts_formatted}",
        }
        if self.qualified:
            schema = (self.schema_name or settings.schema_name or "").strip()
            if schema:
                names = {
                    key: f"{escape_table_name(schema)}.{escape_table_name(name)}"
                    for key, name in names.items()
                }
            else:
                names = {key: escape_table_name(name) for key, name in names.items()}
        object.__setattr__(self, "names", names)

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
        formatter: Callable[[Any], str],
        allow_empty: bool = False,
    ) -> str:
        formatted = formatter(value) if value is not None else formatter("")
        if not isinstance(formatted, str):
            formatted = str(formatted)
        if not formatted and not allow_empty:
            raise ValueError(f"{label} yields an empty value after formatting.")
        return formatted
