from pyspark.sql import SparkSession # type:ignore 
import os

def get_spark_session():

    spark = SparkSession.builder \
                    .appName("entsoe_raw_ingestion") \
                    .master("local[*]") \
                    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
                    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
                    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
                    .config("spark.sql.catalog.nessie.ref", "main") \
                    .config("spark.sql.catalog.nessie.warehouse","s3a://lakehouse") \
                    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
                    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER")) \
                    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD")) \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                    .getOrCreate()
    
    return spark