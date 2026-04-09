from pyspark.sql.functions import col, days # type:ignore

from spark.spark_session import get_spark_session


def transform_data(execution_date: str):

    spark = get_spark_session()

    df = spark.read \
    .table("nessie.raw.entsoe_raw") \
    .filter(col("date") == execution_date)

    transformed_df = df \
        .dropDuplicates(["datetime","country_code","production_type"]) \
        .filter("actual_load_mw > 0") \
        .filter("forecast_load_mw > 0")
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    spark.sql("CREATE TABLE IF NOT EXISTS nessie.silver.entsoe_transformed ( \
                actual_load_mw DOUBLE, \
                country_code STRING, \
                datetime TIMESTAMP, \
                forecast_load_mw DOUBLE, \
                production_type STRING, \
                date DATE \
                ) USING iceberg \
                PARTITIONED BY (days(date))")

    transformed_df.writeTo("nessie.silver.entsoe_transformed") \
        .partitionedBy(days("date")) \
        .append()