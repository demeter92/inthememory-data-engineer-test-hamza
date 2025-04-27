from pyspark.sql import SparkSession
from config.spark_config import conf
from pyspark.sql import functions as F
from scripts.schemas.schema_stores import schema_stores as schema
from config.spark_config import ACCOUNT_NAME,CONTAINER_NAME


spark = (SparkSession.builder 
    .master("spark://spark-master:7077") 
    .appName("bootstrap_stores") 
    .config(conf=conf) 
    .getOrCreate())
stores_file_path = f"wasbs://{CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/stores.csv"


stores_df = (spark.read.format("csv") 
    .option("header", True) 
    .option("sep", ";") 
    .schema(schema)
    .load(stores_file_path))


# Remove parentheses, split by comma, and cast to Float (convert the column to a struct)
stores_df = stores_df.withColumn(
    "latlng_struct",
    F.struct(
        F.regexp_replace(F.split(F.col("latlng"), ",")[0], "[()]", "").cast("float").alias("lat"),
        F.regexp_replace(F.split(F.col("latlng"), ",")[1], "[()]", "").cast("float").alias("lng")
    )
)

# Extract 'lat' and 'lng' from the struct, then drop 'latlng_struct'and 'latlng' columns
stores_df = (stores_df.withColumn("lat", F.col("latlng_struct.lat")) 
             .withColumn("lng", F.col("latlng_struct.lng")) 
             .drop(F.col("latlng_struct"),F.col("latlng")))


#se debarasser des duplicates si elle existe
stores_df = stores_df.dropDuplicates()

#####Ajouter ingestion_ts + extraire les colonnes de partition date_yyyy_mm_dd et hour pour les besoins d'audit
stores_df = stores_df.withColumn(
    "ingestion_ts", F.current_timestamp()
)
#crer la DB si elle n'existe pas 
spark.sql("CREATE DATABASE IF NOT EXISTS hive_prod.db")

#creer la table si elle n'existe pas
spark.sql("""
    CREATE TABLE IF NOT EXISTS hive_prod.db.stores (
        id STRING,
        lat INTEGER,
        lng INTEGER,
        opening INTEGER,
        closing INTEGER,
        type INTEGER,
        ingestion_ts TIMESTAMP

    )
    USING iceberg
""")

# Enregistrer sous une table Iceberg
(stores_df.write.format("iceberg") 
    .mode("overwrite") 
    .save("hive_prod.db.stores"))
