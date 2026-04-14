import polars as pl


df = pl.read_csv('/home/costperclick/PycharmProjects/AOL_query_leak_analysis/data/results/categorized.csv')

df = df.filter(
    pl.col('category') != "other"
)

behaviour = df.group_by(['AnonID', 'category']).agg(
    pl.col('category').count().alias('count_by_category')
).sort(pl.col('count_by_category'), descending=True)

print(behaviour)

user_inspect = df.filter(
    (pl.col('category') == 'adult_sexual')
)

user_inspect.write_csv('naughtboys.csv')