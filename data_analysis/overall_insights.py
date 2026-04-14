import polars as pl
from polars import LazyFrame
from datetime import datetime
from config import *


def load_csv(filepath: str) -> LazyFrame:
    lf = pl.scan_csv(filepath)
    return lf


def overall_metrics(lf: LazyFrame) -> pl.DataFrame:
    output = (lf.with_columns(
        pl.col("QueryTime").str.to_datetime(strict=False)
    ).with_columns(
        pl.col("QueryTime").dt.hour().alias('query_hour'),
    ).group_by('category').agg(
        pl.col('category').count().alias('category_redundancy'),
        pl.col('AnonID').unique().count().alias('unique_users_for_category'),
        pl.col('Query').count().mean().alias('avg_query_amount_for_category'),
        pl.col('query_hour').mean().alias('avg_category_hourtime'),
    ).collect(engine='streaming').sort(
        pl.col('category_redundancy'), descending=True
    ))

    overall_sum = output['category_redundancy'].sum()

    output = output.with_columns(
        (pl.col('category_redundancy') / overall_sum).round(4).alias('% category share'),
        (pl.col('category_redundancy') / pl.col('unique_users_for_category')).alias('avg_query_amount_per_users'),
    )


    return output


if __name__ == '__main__':
    fii = pl.read_csv(CATEGORISED_DATA)
    print(fii.head())
    foo = load_csv(CATEGORISED_DATA)
    overall = overall_metrics(foo)
    overall.write_csv(RESULTS_DIR / "overall.csv")
