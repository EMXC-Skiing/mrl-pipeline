"""Shared lifecycle table naming helpers."""

from __future__ import annotations

import re
from datetime import datetime, timezone


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
