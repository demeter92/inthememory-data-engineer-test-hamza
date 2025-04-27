from pyspark.sql import SparkSession
from config.spark_config import conf
from scripts.schemas.schema_products import schema_products as schema
from config.spark_config import ACCOUNT_NAME,CONTAINER_NAME
from pyspark.sql import functions as F


spark = (SparkSession.builder 
    .master("spark://spark-master:7077") 
    .appName("bootstrap_products") 
    .config(conf=conf) 
    .getOrCreate())

prodcuts_file_path = f"wasbs://{CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/products.csv"


products_df = (spark.read.format("csv") 
    .option("header", True) 
    .option("sep", ";") 
    .schema(schema)
    .load(prodcuts_file_path))

#se debarasser des duplicates si elle existe
products_df = products_df.dropDuplicates()

#####Ajouter ingestion_ts + extraire les colonnes de partition date_yyyy_mm_dd et hour pour les besoins d'audit
products_df = products_df.withColumn(
    "ingestion_ts", F.current_timestamp()
)

#crer la DB si elle n'existe pas 
spark.sql("CREATE DATABASE IF NOT EXISTS hive_prod.db")

#creer la table si elle n'existe pas
spark.sql("""
    CREATE TABLE IF NOT EXISTS hive_prod.db.products (
        id STRING,
        ean STRING,
        brand STRING,
        description STRING,
        ingestion_ts TIMESTAMP
    )
    USING iceberg
""")


# Enregistrer sous une table Iceberg
(products_df.write.format("iceberg") 
    .mode("overwrite") 
    .save("hive_prod.db.products"))

#de point de vue instanctiation , on peut visualiser les données de la table iceberg
#dans minio à l'endroit suivant :
#iceberg/db.db/products
#ou  bien via une requete sql en utilisant trino  comme ceci :
#SELECT * from iceberg.db.products;