from pyspark.sql import SparkSession
import os

print("Starting SparkSession...")

spark = SparkSession.builder \
                .appName("test_spark_nessie") \
                .master("local[*]") \
                .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
                .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
                .config("spark.sql.catalog.nessie.ref", "main") \
                .config("spark.sql.catalog.nessie.warehouse","s3a://raw/iceberg") \
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
                .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER")) \
                .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD")) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("SparkSession created successfully")
print(f"Spark version: {spark.version}")

print("Creating NAMESPACE...")
spark.sql('CREATE NAMESPACE IF NOT EXISTS nessie.testdb') # Namespace = Database dentro del catalog
print("Namespace created successfully")

print("Creating testing TABLE...")
spark.sql('CREATE TABLE IF NOT EXISTS nessie.testdb.testtbl(id bigint,data string) USING iceberg')
print("Table created successfully")

print("Inserting data...")
spark.sql('INSERT INTO nessie.testdb.testtbl VALUES(1,"Everything"),(2,"is"),(3,"running")')

print("Reading data...")
spark.sql('SELECT data FROM nessie.testdb.testtbl').show()

# Setting el garbage collector a true para poder eliminar la tabla. Warn sobre la herramienta de nessie-gc por su funcionamiento gitlike, pero es sólo un test
spark.sql("ALTER TABLE nessie.testdb.testtbl SET TBLPROPERTIES ('gc.enabled'='true')")

print("Deleting TABLE and PURGING data...")
spark.sql('DROP TABLE nessie.testdb.testtbl PURGE')

print("Deleting NAMESPACE...")
spark.sql('DROP NAMESPACE nessie.testdb')

spark.stop()

print("Test completed successfully")