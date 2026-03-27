from spark.spark_session import get_spark_session
from spark.utils import chop_date

def verify_raw(execution_date):

    spark = get_spark_session()
    
    year, month, day = chop_date(execution_date)

    path = f"s3a://raw/entsoe/year={year}/month={month}/day={day}"
    
    df = spark.read.parquet(path)

    count = df.count()
    
    if count == 0:
        raise ValueError(f"No data written for {execution_date}")
    
    print(f"Verified: {count} rows written for {execution_date}")