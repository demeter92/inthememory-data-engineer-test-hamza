import pyspark
from pyspark.sql import SparkSession
import os

## DEFINE SENSITIVE VARIABLES
WAREHOUSE = "s3a://iceberg/" ## BUCKET TO WRITE DATA TO
AWS_REGION = "us-east-1"
AWS_ACCESS_KEY = 'admin' ## AWS CREDENTIALS
AWS_SECRET_KEY = 'admin123' ## AWS CREDENTIALS
ACCOUNT_NAME = os.getenv("ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
conf = (
    pyspark.SparkConf()
        .set('spark.jars.packages',
             'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,'
             'org.apache.iceberg:iceberg-hive-runtime:1.4.2,'
             'org.apache.hadoop:hadoop-aws:3.3.1,'
             'com.amazonaws:aws-java-sdk-bundle:1.11.1026,'
             'org.apache.hadoop:hadoop-azure:3.3.2,'
             'com.microsoft.azure:azure-storage:8.6.6'
             )

        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .set('spark.sql.catalog.hive_prod', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.hive_prod.type', 'hive')
        .set('spark.sql.catalog.hive_prod.uri', 'thrift://metastore:9083')
        .set('spark.sql.catalog.hive_prod.warehouse', WAREHOUSE)
        .set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)  
        .set("spark.hadoop.fs.s3a.region", AWS_REGION) 
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.aws.credential.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialProvider")
        .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.executor.memory","4G")
        .set(f"fs.azure.account.key.{ACCOUNT_NAME}.blob.core.windows.net",ACCOUNT_KEY)

)


