from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_FILE = DATA_DIR / "raw_aol_leak.txt"
CATEGORISED_DATA_FILE = DATA_DIR / "data_categorised.csv"
DATA_RESULT = DATA_DIR / "results"
