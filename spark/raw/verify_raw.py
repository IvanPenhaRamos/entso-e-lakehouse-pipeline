from spark.spark_session import get_spark_session

def verify_raw(execution_date):

    spark = get_spark_session()

    
    year = execution_date[:4]
    month = execution_date[5:7]
    day = execution_date[8:10]

    path = f"s3a://raw/entsoe/year={year}/month={month}/day={day}"
    
    df = spark.read.parquet(path)

    count = df.count()
    
    if count == 0:
        raise ValueError(f"No data written for {execution_date}")
    
    print(f"Verified: {count} rows written for {execution_date}")