import polars as pl
from config import DATA_DIR, RAW_DATA_FILE, CATEGORISED_DATA_FILE, DATA_RESULT
import time

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


def categorize(lf: pl.LazyFrame, cat_filepath: str) -> pl.DataFrame:
    lf_categories = (pl.scan_csv(cat_filepath)
    .unique(maintain_order=True)
    .with_columns(
        pl.col("Query")
        .str.to_lowercase()
        .str.normalize("NFKD")
        .str.replace_all(r"\p{Mn}", "")
        .str.strip_chars()
        .alias("Query_key")
    ))

    merged_df = lf.join(
        lf_categories,
        left_on='Query',
        right_on='Query_key',
        how='left'
    ).collect(engine="streaming").sort(pl.col("QueryTime"), descending=True)

    return merged_df


def ensure_result_dir():
    DATA_RESULT.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    ensure_result_dir()

    start = time.time()
    raw_csv = RAW_DATA_FILE
    categories_filepath = CATEGORISED_DATA_FILE
    parsed = parse_data(RAW_DATA_FILE)
    normed = normalize_data(parsed)
    categorized = categorize(normed, categories_filepath)
    categorized.write_csv(DATA_RESULT / "categorized.csv")
    end = time.time()
    print(f"Total time: {end - start}")
