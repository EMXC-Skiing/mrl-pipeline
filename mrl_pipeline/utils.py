import pathlib
import re

# Paths to project directories
ROOT_DIR = pathlib.Path(__file__).parent.parent
DB_DIR = ROOT_DIR / "db"
DATA_DIR = ROOT_DIR / "data"
CONFIG_DIR = ROOT_DIR / "config"
LOG_DIR = ROOT_DIR / "logs"

google_service_account_path = CONFIG_DIR / "service.json"
duckdb_path = DB_DIR / "mrl.duckdb"


def sanitize_table_name(name: str) -> str:
    """Sanitizes and validates a table name, allowing only alphanumeric characters,
    underscores, and plus signs.

    Args:
        name (str): The original table name.

    Returns:
        str: The sanitized table name.

    """
    return re.sub(r"[^a-zA-Z0-9_+]", "", name)  # Remove invalid characters
