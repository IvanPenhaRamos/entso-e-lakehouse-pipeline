from pyspark.sql.functions import to_date, col, days #type:ignore

from spark.raw.generate_data import generate_data
from spark.spark_session import get_spark_session

def write_raw(execution_date: str):
    
    spark = get_spark_session() # Only to ensure SparkSession initialized
    
    df = generate_data(execution_date).withColumn("date", to_date(col("datetime")))

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw")

    spark.sql("CREATE TABLE IF NOT EXISTS nessie.raw.entsoe_raw ( \
            actual_load_mw DOUBLE, \
            country_code STRING, \
            datetime TIMESTAMP, \
            forecast_load_mw DOUBLE, \
            production_type STRING, \
            date DATE \
            ) USING iceberg \
            PARTITIONED BY (days(date))")

    df.writeTo("nessie.raw.entsoe_raw") \
        .partitionedBy(days("date")) \
        .append()
