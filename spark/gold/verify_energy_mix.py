from pyspark.sql.functions import col #type:ignore

from spark.spark_session import get_spark_session

def verify_energy_mix(execution_date):

    spark = get_spark_session()

    df = spark.table("nessie.gold.energy_mix_per_country")

    today_df = df.where(col("date") == execution_date)
    
    rows_count = today_df.count()
    
    if rows_count == 0:
        raise ValueError(f"No data written for {execution_date}")
    
    if rows_count != 42:
        raise ValueError(f"Only {rows_count} were found, instead 42")
    
    pct_sum_df = today_df \
        .groupBy("country_code") \
        .sum("pct_of_total")
    
    wrong_pct_count = pct_sum_df \
        .filter(col("sum(pct_of_total)") < 0.99) \
        .count()
    
    if wrong_pct_count != 0:
        raise ValueError(f"{wrong_pct_count} countries don't match with the total %")

    print(f"energy_mix_per_country verified: {rows_count} rows for {execution_date}")