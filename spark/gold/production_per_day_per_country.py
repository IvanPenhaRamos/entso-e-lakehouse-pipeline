from pyspark.sql.functions import col, days #type:ignore

from spark.spark_session import get_spark_session

def production_per_day_per_country(execution_date):

    spark = get_spark_session()

    df = spark.read \
        .table("nessie.silver.entsoe_transformed") \
        .filter(col("date") == execution_date)
    
    df.createOrReplaceTempView("production_view")

    query = "SELECT date, \
                    country_code, \
                    ROUND(SUM(actual_load_mw),2) AS total_actual_mw, \
                    ROUND(MAX(actual_load_mw),2) AS max_actual_mw,\
                    ROUND(MIN(actual_load_mw),2) AS min_actual_mw \
            FROM production_view \
            GROUP BY date, \
                    country_code"

    gold_df = spark.sql(query)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    spark.sql("CREATE TABLE IF NOT EXISTS nessie.gold.production_per_day_per_country ( \
                date DATE, \
                country_code STRING, \
                total_actual_mw DOUBLE, \
                max_actual_mw DOUBLE, \
                min_actual_mw DOUBLE \
                ) USING iceberg \
                PARTITIONED BY (days(date))")

    gold_df.writeTo("nessie.gold.production_per_day_per_country") \
            .partitionedBy(days("date")) \
            .append()
    