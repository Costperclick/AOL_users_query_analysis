import polars as pl
import re

df = pl.read_csv('/home/costperclick/PycharmProjects/AOL_query_leak_analysis/data/results/categorized.csv')


beast_pattern = r"\b(sex with dog|animal sex)\b"
kids_pattern = r"\b(children sex| kids sex | pedophile | child porn)\b"



dangerous_flag = df.with_columns(
    pl.col('Query').str.contains(beast_pattern).alias("bestiality"),
    pl.col('Query').str.contains(kids_pattern).alias("kids"),
)

dangers = dangerous_flag.filter(
    pl.col('bestiality') == True
)

individuals = dangers['AnonID'].unique().to_list()

temp_df = []
for individual in individuals:
    foo = df.filter(
        pl.col('AnonID') == individual
    )
    temp_df.append(foo)

new_df = pl.concat(temp_df)

new_df.write_csv('dangerous_ones.csv')

