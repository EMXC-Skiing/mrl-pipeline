"""Utilities for shared paths, configuration helpers, and small helpers."""

import json
import os
import pathlib
import re

from dotenv import load_dotenv

# Paths to project directories
ROOT_DIR = pathlib.Path(__file__).parent.parent.parent
DB_DIR = ROOT_DIR / "db"
DATA_DIR = ROOT_DIR / "data"
CONFIG_DIR = ROOT_DIR / "config"
LOG_DIR = ROOT_DIR / "logs"

duckdb_path = DB_DIR / "mrl.duckdb"


def sanitize_table_name(name: str) -> str:
    """Sanitizes and validates a table name, allowing only alphanumeric characters,
    underscores, and plus signs.

    Args:
        name (str): The original table name.

    Returns:
        str: The sanitized table name.

    """
    stripped = re.sub(r"[^a-zA-Z0-9_+]", "", name)  # Remove invalid characters
    return stripped.lower()  # Convert to lowercase


def fetch_environ(name: str) -> str:
    """Retrieve configuration values from the environment or module globals.

    The lookup prefers OS environment variables (after loading ``.env``) and
    falls back to module-level globals when no environment value is present.

    Args:
        name: Name of the configuration variable to load.

    Returns:
        The resolved configuration value. JSON content is parsed when possible.

    Raises:
        RuntimeError: If no non-empty value can be found.

    """

    load_dotenv()

    def _decode_json(value: object):
        if isinstance(value, pathlib.Path):
            value = value.read_text()
        if isinstance(value, bytes):
            value = value.decode()

        try:
            return json.loads(str(value))
        except (TypeError, json.JSONDecodeError):
            return value

    value = os.getenv(name)
    if name in os.environ:
        return _decode_json(value)

    fallback = globals().get(name)
    if fallback is not None:
        return _decode_json(fallback)

    raise RuntimeError(f"Missing required configuration variable '{name}'.")
