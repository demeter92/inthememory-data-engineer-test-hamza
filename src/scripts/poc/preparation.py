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


schema = StructType([
    StructField("State", StringType(), False),
    StructField("Color", StringType(), False),
    StructField("Count", IntegerType(), False)
])

df =  (spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load("file:///app/shared_storage/test_data/mnm_dataset.csv"))


df_count = (df
            .select(F.col("State"), F.col("Color"), F.col("Count"))
            .groupBy("State", "Color")
            .sum("Count")
            .orderBy("sum(Count)", ascending=False)
            )
ipdb.set_trace()

df_count.show(truncate=False)
