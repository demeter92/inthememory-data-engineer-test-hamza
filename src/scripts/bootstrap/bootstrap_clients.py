from pyspark.sql import SparkSession
from config.spark_config import conf
from scripts.schemas.schema_clients import schema_clients as schema

spark = (SparkSession.builder 
    .master("spark://spark-master:7077") 
    .appName("bootstrap_clients") 
    .config(conf=conf) 
    .getOrCreate())


clients_file_path = "file:///app/shared_storage/data/clients.csv"


clients_df = (spark.read.format("csv") 
    .option("header", True) 
    .option("sep", ";") 
    .schema(schema)
    .load(clients_file_path))

#se debarasser des duplicates si elle existe
#deux clients se sont perdus car id dupliqué 
clients_df = clients_df.dropDuplicates(['id'])



#crer la DB si elle n'existe pas 
spark.sql("CREATE DATABASE IF NOT EXISTS hive_prod.db")

#creer la table si elle n'existe pas
spark.sql("""
    CREATE TABLE IF NOT EXISTS hive_prod.db.clients (
        id STRING,
        name STRING,
        job STRING,
        email STRING,
        account_id STRING
    )
    USING iceberg
""")

# Enregistrer sous une table Iceberg
(clients_df.write.format("iceberg") 
    .mode("overwrite") 
    .save("hive_prod.db.clients"))