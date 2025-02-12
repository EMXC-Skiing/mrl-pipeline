import pathlib

# Paths to project directories
ROOT_DIR = pathlib.Path(__file__).parent.parent
DB_DIR = ROOT_DIR / "db"
DATA_DIR = ROOT_DIR / "data"
CONFIG_DIR = ROOT_DIR / "config"
LOG_DIR = ROOT_DIR / "logs"

google_service_account_credentials = CONFIG_DIR / "service.json"
