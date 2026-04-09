from spark_session import get_spark_session

spark = get_spark_session()


# RAW
spark.sql("ALTER TABLE nessie.raw.entsoe_raw \
          SET TBLPROPERTIES ('gc.enabled'='true')")

spark.sql("DROP TABLE nessie.raw.entsoe_raw PURGE")

# SILVER
spark.sql("ALTER TABLE nessie.silver.entsoe_transformed \
          SET TBLPROPERTIES ('gc.enabled'='true')")

spark.sql("DROP TABLE nessie.silver.entsoe_transformed PURGE")

# GOLD
spark.sql("ALTER TABLE nessie.gold.production_per_day_per_country \
          SET TBLPROPERTIES ('gc.enabled'='true')")

spark.sql("ALTER TABLE nessie.gold.forecast_accuracy \
          SET TBLPROPERTIES ('gc.enabled'='true')")

spark.sql("ALTER TABLE nessie.gold.energy_mix_per_country \
          SET TBLPROPERTIES ('gc.enabled'='true')")

spark.sql("DROP TABLE nessie.gold.production_per_day_per_country PURGE")

spark.sql("DROP TABLE nessie.gold.forecast_accuracy PURGE")

spark.sql("DROP TABLE nessie.gold.energy_mix_per_country PURGE")
