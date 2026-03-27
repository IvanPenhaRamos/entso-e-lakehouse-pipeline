from pyspark.sql.functions import days, months, years #type:ignore

from spark.spark_session import get_spark_session
from spark.utils import chop_date

def production_per_day_per_country(execution_date):

    spark = get_spark_session()

    year, month, day = chop_date(execution_date)

    df = spark.read \
    .option("basePath", "s3a://silver/entsoe/") \
    .parquet(f"s3a://silver/entsoe/year={year}/month={month}/day={day}")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    df.createOrReplaceTempView("gold_table")

    query = "SELECT to_date(datetime)   AS date, \
                    country_code, \
                    ROUND(SUM(actual_load_mw),2) AS total_actual_mw, \
                    ROUND(MAX(actual_load_mw),2) AS max_actual_mw,\
                    ROUND(MIN(actual_load_mw),2) AS min_actual_mw \
            FROM gold_table \
            GROUP BY to_date(datetime), \
                    country_code"

    gold_df = spark.sql(query)

    gold_df.writeTo("nessie.gold.production_per_day_per_country") \
            .partitionedBy(days("date")) \
            .createOrReplace()