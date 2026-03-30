from pyspark.sql.functions import days #type:ignore

from spark.spark_session import get_spark_session
from spark.utils import chop_date

def forecast_accuracy(execution_date):

    spark = get_spark_session()

    year, month, day = chop_date(execution_date)

    df = spark.read \
    .option("basePath", "s3a://silver/entsoe/") \
    .parquet(f"s3a://silver/entsoe/year={year}/month={month}/day={day}")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    df.createOrReplaceTempView("forecast_accuracy_view")

    query = "SELECT to_date(datetime) AS date, \
                    country_code, \
                    SUM(actual_load_mw) AS total_actual_load_mw, \
                    SUM(forecast_load_mw) AS total_forecast_load_mw, \
                    ROUND(AVG(ABS(actual_load_mw - forecast_load_mw)), 2) AS mae, \
                    ROUND(AVG(ABS(actual_load_mw - forecast_load_mw) / actual_load_mw * 100), 2) AS mape, \
                    ROUND(AVG(actual_load_mw - forecast_load_mw), 2) AS bias \
            FROM forecast_accuracy_view \
            GROUP BY to_date(datetime), \
                    country_code"

    gold_df = spark.sql(query)

    gold_df.writeTo("nessie.gold.forecast_accuracy") \
            .partitionedBy(days("date")) \
            .createOrReplace()
    