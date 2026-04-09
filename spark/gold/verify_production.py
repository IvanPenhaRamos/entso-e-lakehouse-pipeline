from pyspark.sql.functions import col #type:ignore

from spark.spark_session import get_spark_session

def verify_production(execution_date):

    spark = get_spark_session()

    df = spark.table("nessie.gold.production_per_day_per_country")

    today_df = df.where(col("date") == execution_date)
    
    rows_count = today_df.count()
    
    if rows_count == 0:
        raise ValueError(f"No data written for {execution_date}")
    
    country_count = today_df.select("country_code").distinct().count()
    
    if country_count != 6:
        raise ValueError(f"Only {country_count} countries were included")
    
    print(f"production_per_day_per_country verified: {rows_count} rows written for {execution_date}")