from pyspark.sql.functions import col #type:ignore

from spark.spark_session import get_spark_session

def verify_raw(execution_date: str):

    spark = get_spark_session()
    
    df = spark.read.table("nessie.raw.entsoe_raw")

    today_df = df.filter(col("date") == execution_date)

    count = today_df.count()
    
    if count == 0:
        raise ValueError(f"No data written for {execution_date}")
    
    print(f"Verified: {count} rows written for {execution_date}")
