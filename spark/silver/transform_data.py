from pyspark.sql.functions import col, to_timestamp # type:ignore

from spark.spark_session import get_spark_session
from spark. utils import chop_date

def transform_data(execution_date: str):

    spark = get_spark_session()

    year, month, day = chop_date(execution_date)

    df = spark.read \
    .option("basePath", "s3a://raw/entsoe/") \
    .parquet(f"s3a://raw/entsoe/year={year}/month={month}/day={day}")

    transformed_df = df \
        .withColumn("datetime",to_timestamp(col("datetime"),"yyyy-MM-dd HH:mm:ss")) \
        .dropDuplicates(["datetime","country_code","production_type"]) \
        .filter("actual_load_mw > 0") \
        .filter("forecast_load_mw > 0")
    
    transformed_df.write.mode("overwrite").partitionBy("year","month","day").parquet("s3a://silver/entsoe/")