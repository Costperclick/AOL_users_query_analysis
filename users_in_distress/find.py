import polars as pl

df = pl.read_csv('/home/costperclick/PycharmProjects/AOL_query_leak_analysis/data/results/categorized.csv')

suicide_pattern = r"\b(suicide|kill myself|kill my self|killing myself)\b"
cancer_pattern = r"\b(cancer|dying of cancer|cancer killing me|i have cancer|having cancer|living with cancer)\b"
lost_job_pattern = r"\b(lost my job|i got fired|fired from work|losing my job|job loss|laid off)\b"
depression_pattern = r"\b(i'm depressed|depression|feeling lonely|i'm always sad|too depressed)\b"

distress_flag = df.with_columns(
    pl.col('Query').fill_null("").str.contains(suicide_pattern).alias("suicidal_thoughts"),
    pl.col('Query').fill_null("").str.contains(cancer_pattern).alias("cancer"),
    pl.col('Query').fill_null("").str.contains(lost_job_pattern).alias("lost_job"),
    pl.col('Query').fill_null("").str.contains(depression_pattern).alias("depression"),
)

warnings = distress_flag.filter(
    pl.any_horizontal(
        "suicidal_thoughts",
        "cancer",
        "lost_job",
        "depression"
    )
)

individuals = warnings.select("AnonID").unique()

new_df = df.join(individuals, on="AnonID", how="inner")

new_df.write_csv('suicidals_ones.csv')
