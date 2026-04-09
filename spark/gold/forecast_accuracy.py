from pyspark.sql.functions import col, days #type:ignore

from spark.spark_session import get_spark_session

def forecast_accuracy(execution_date):

    spark = get_spark_session()

    df = spark.read \
        .table("nessie.silver.entsoe_transformed") \
        .filter(col("date") == execution_date)

    df.createOrReplaceTempView("forecast_accuracy_view")

    query = "SELECT date, \
                    country_code, \
                    SUM(actual_load_mw) AS total_actual_load_mw, \
                    SUM(forecast_load_mw) AS total_forecast_load_mw, \
                    ROUND(AVG(ABS(actual_load_mw - forecast_load_mw)), 2) AS mae, \
                    ROUND(AVG(ABS(actual_load_mw - forecast_load_mw) / actual_load_mw * 100), 2) AS mape, \
                    ROUND(AVG(actual_load_mw - forecast_load_mw), 2) AS bias \
            FROM forecast_accuracy_view \
            GROUP BY date, \
                    country_code"

    gold_df = spark.sql(query)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    spark.sql("CREATE TABLE IF NOT EXISTS nessie.gold.forecast_accuracy( \
                date DATE, \
                country_code STRING, \
                total_actual_load_mw DOUBLE, \
                total_forecast_load_mw DOUBLE, \
                mae DOUBLE, \
                mape DOUBLE, \
                bias DOUBLE \
                )USING iceberg \
                PARTITIONED BY (days(date))")

    gold_df.writeTo("nessie.gold.forecast_accuracy") \
            .partitionedBy(days("date")) \
            .append()

    