from spark.spark_session import get_spark_session
from spark.utils import chop_date

def verify_silver(execution_date):

    spark = get_spark_session()

    year, month, day = chop_date(execution_date)

    df = spark.read.option("basePath", "s3a://silver/entsoe/") \
        .parquet(f"s3a://silver/entsoe/year={year}/month={month}/day={day}")

    count = df.count()
    
    if count == 0:
        raise ValueError(f"No data written for {execution_date}")
    
    print(f"Verified: {count} rows written for {execution_date}")
