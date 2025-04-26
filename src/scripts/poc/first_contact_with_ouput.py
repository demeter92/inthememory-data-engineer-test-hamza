from pyspark.sql import SparkSession
from config.spark_config import conf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
spark = (SparkSession.builder 
    .master("spark://spark-master:7077") 
    .appName("read-output") 
    .config(conf=conf) 
    .getOrCreate())


# Path to the directory containing Parquet files
parquet_directory = "/app/shared_storage/data/database/clients"

import ipdb
ipdb.set_trace()

# Read the Parquet files into a DataFrame
df = (spark
      .read
      .option("compression", "SNAPPY")
      .parquet(parquet_directory))

# Show a sample of the data
df.show()
