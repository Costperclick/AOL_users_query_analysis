from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RESULTS_DIR = DATA_DIR / "results"
CATEGORISED_DATA = RESULTS_DIR / "categorized.csv"

print(BASE_DIR)
print(CATEGORISED_DATA)