from pyspark.sql import SparkSession
from config.spark_config import conf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import ipdb
spark = (SparkSession.builder 
    .master("spark://spark-master:7077") 
    .appName("preparation") 
    .config(conf=conf) 
    .getOrCreate())


df =  (spark.read.format("csv")
    .option("header", "true")
    # .schema(schema)
    .load("file:///app/shared_storage/data/transactions_2023-11-23_9.csv"))

ipdb.set_trace()
df.show(truncate=False)