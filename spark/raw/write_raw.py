from spark.raw.generate_data import generate_data
from spark.spark_session import get_spark_session

def write_raw(execution_date: str):
    
    get_spark_session() # Only to ensure SparkSession initialized
    
    df = generate_data(execution_date)

    df.write.mode("overwrite").partitionBy("year","month","day").parquet("s3a://raw/entsoe/")