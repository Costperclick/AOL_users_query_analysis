import polars as pl
import re

df = pl.read_csv('/home/costperclick/PycharmProjects/AOL_query_leak_analysis/data/results/categorized.csv')


suicide_pattern = r"\b(suicide | kill myself | kill my self | depression | depressed | killing myself | kill myself)\b"
cancer_pattern = r"\b(cancer | dying of cancer | cancer killing me | i have cancer | having cancer | living with cancer)\b"



dangerous_flag = df.with_columns(
    pl.col('Query').str.contains(suicide_pattern).alias("suicidal_thoughts"),
    pl.col('Query').str.contains(cancer_pattern).alias("cancer")
)

dangers = dangerous_flag.filter(
    pl.col('suicidal_thoughts') == True,
    pl.col("cancer") == True
)

individuals = dangers['AnonID'].unique().to_list()

temp_df = []
for individual in individuals:
    foo = df.filter(
        pl.col('AnonID') == individual
    )
    temp_df.append(foo)

new_df = pl.concat(temp_df)

new_df.write_csv('suicidals_ones.csv')

