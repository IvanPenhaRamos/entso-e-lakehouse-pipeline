from pyspark.sql.window import Window #type:ignore
from pyspark.sql.functions import to_date, col, sum, days #type:ignore

from spark.spark_session import get_spark_session
from spark.utils import chop_date


def energy_mix_per_country(execution_date):

    spark = get_spark_session()

    year, month, day = chop_date(execution_date)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    df = spark.read \
    .option("basePath", "s3a://silver/entsoe/") \
    .parquet(f"s3a://silver/entsoe/year={year}/month={month}/day={day}")

    datetime_df = df.withColumn("date",to_date(col("datetime"),"yyyy-MM-dd"))
    
    window_country_day = Window.partitionBy("date","country_code")
    window_type_day = Window.partitionBy("date", "country_code", "production_type")

    energy_mix_df =  datetime_df \
        .withColumn("country_total_mw", sum("actual_load_mw").over(window_country_day)) \
        .withColumn("total_per_type_mw", sum("actual_load_mw").over(window_type_day)) \
        .withColumn("pct_of_total", col("total_per_type_mw")/col("country_total_mw")) \
        .select("date", "country_code", "production_type", "country_total_mw", "total_per_type_mw", "pct_of_total") \
        .distinct()
    
    energy_mix_df.writeTo("nessie.gold.energy_mix_per_country") \
        .partitionedBy(days("date")) \
        .createOrReplace()