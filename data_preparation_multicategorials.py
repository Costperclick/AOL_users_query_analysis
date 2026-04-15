"""
This approach can generate multiple categories for the same queries, hence it's more accurate.
"""

import polars as pl
import time

from data.categories_regex import CATEGORIES_POLARS
from config import DATA_DIR, RAW_DATA_FILE, CATEGORISED_DATA_FILE, DATA_RESULT


def parse_data(filepath: str) -> pl.LazyFrame:
    lf = pl.scan_csv(filepath, separator='\t', quote_char=None)
    lf = lf.with_columns(pl.col("QueryTime").str.to_datetime(format='%Y-%m-%d %H:%M:%S')
                         )
    return lf


def normalize_data(lf: pl.LazyFrame) -> pl.LazyFrame:
    lf = lf.with_columns(
        pl.col("Query").str.to_lowercase().str.normalize("NFKD").str.replace_all(r"\p{Mn}", "")
    ).select(["AnonID", "QueryTime", "Query"])

    return lf


def categorize(lf: pl.LazyFrame) -> pl.DataFrame:
    cats = list(CATEGORIES_POLARS.keys())
    lf = lf.with_columns([
        pl.col("Query").str.contains(pattern).alias(cat)
        for cat, pattern in CATEGORIES_POLARS.items()
    ]).with_columns(
        pl.concat_list([
            pl.when(pl.col(cat)).then(pl.lit(cat)).otherwise(pl.lit(None))
            for cat in cats
        ]).list.drop_nulls().alias("categories")
    ).drop(cats).with_columns(
        pl.col("categories").list.join(", ")
    )

    df = lf.collect(engine="streaming")
    return df


def ensure_result_dir():
    DATA_RESULT.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    ensure_result_dir()

    start = time.time()
    raw_csv = RAW_DATA_FILE
    parsed = parse_data(RAW_DATA_FILE)
    normed = normalize_data(parsed)
    categorized = categorize(normed)
    categorized.write_csv(DATA_RESULT / "categorized_2.csv")
    end = time.time()
    print(f"Total time: {end - start}")
