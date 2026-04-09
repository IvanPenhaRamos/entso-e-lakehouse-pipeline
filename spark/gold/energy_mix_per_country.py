from pyspark.sql.window import Window #type:ignore
from pyspark.sql.functions import col, sum, days #type:ignore

from spark.spark_session import get_spark_session

def energy_mix_per_country(execution_date):

    spark = get_spark_session()

    df = spark.read \
        .table("nessie.silver.entsoe_transformed") \
        .filter(col("date") == execution_date)
   
    window_country_day = Window.partitionBy("date","country_code")
    window_type_day = Window.partitionBy("date", "country_code", "production_type")

    energy_mix_df =  df \
        .withColumn("country_total_mw", sum("actual_load_mw").over(window_country_day)) \
        .withColumn("total_per_type_mw", sum("actual_load_mw").over(window_type_day)) \
        .withColumn("pct_of_total", col("total_per_type_mw")/col("country_total_mw")) \
        .select("date", "country_code", "production_type", "country_total_mw", "total_per_type_mw", "pct_of_total") \
        .distinct()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    spark.sql("CREATE TABLE IF NOT EXISTS nessie.gold.energy_mix_per_country( \
                date DATE, \
                country_code STRING, \
                production_type STRING, \
                country_total_mw DOUBLE, \
                total_per_type_mw DOUBLE, \
                pct_of_total DOUBLE\
              ) USING iceberg \
              PARTITIONED BY (days(date))")
    
    energy_mix_df.writeTo("nessie.gold.energy_mix_per_country") \
        .partitionedBy(days("date")) \
        .append()