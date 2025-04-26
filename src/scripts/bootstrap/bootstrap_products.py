from pyspark.sql import SparkSession
from config.spark_config import conf
from pyspark.sql.types import StructType, StructField, StringType


spark = (SparkSession.builder 
    .master("spark://spark-master:7077") 
    .appName("bootstrap_products") 
    .config(conf=conf) 
    .getOrCreate())

prodcuts_file_path = "file:///app/shared_storage/data/products.csv"
#je prefere string pour id et ean si jamais il existe des 00 au debut comme le cas de sirene
#je considere que l'id et l'ean ne peuvent pas etre null
schema = StructType([      
    StructField("id", StringType(), False),
    StructField("ean", StringType(), False),
    StructField("brand", StringType(), True),
    StructField("description", StringType(), True)
])


products_df = (spark.read.format("csv") \
    .option("header", True) 
    .option("sep", ";") 
    .schema(schema)
    .load(prodcuts_file_path))

#se debarasser des duplicates si elle existe
products_df = products_df.dropDuplicates(["ean","id"])

#crer la DB si elle n'existe pas 
spark.sql("CREATE DATABASE IF NOT EXISTS hive_prod.db")

#creer la table si elle n'existe pas
spark.sql("""
    CREATE TABLE IF NOT EXISTS hive_prod.db.products (
        id STRING,
        ean STRING,
        brand STRING,
        description STRING
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