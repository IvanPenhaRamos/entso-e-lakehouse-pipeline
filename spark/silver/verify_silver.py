from pyspark.sql.functions import col #type:ignore

from spark.spark_session import get_spark_session

def verify_silver(execution_date):

    spark = get_spark_session()

    df = spark.read \
        .table("nessie.silver.entsoe_transformed") \
        .filter(col("date") == execution_date)

    count = df.count()
    
    if count == 0:
        raise ValueError(f"No data written for {execution_date}")
    
    print(f"Verified: {count} rows written for {execution_date}")
