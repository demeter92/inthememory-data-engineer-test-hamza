
import logging
from pyspark.sql import SparkSession
from config.spark_config import conf
from pyspark.sql import functions as F
from scripts.schemas.schema_transactions import schema_transactions as schema
from scripts.utils.match_transactions_files import list_matching_transaction_files_azure
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from pyspark.sql.functions import current_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-warning")




# the date of today as a string in the format YYYY-MM-DD
###that would be the official
today_str = datetime.today().strftime("%Y-%m-%d")

#read transactions of the day from the shared storage
transactions_full_path = list_matching_transaction_files_azure(date=today_str) #for production
#transactions_full_path = list_matching_transaction_files_azure(date="2023-11-25") #for testing
#transactions_full_path = list_matching_transaction_files_azure(date="2023-11-25",hour="8") # fir testing
if not transactions_full_path:
    logger.error(f"No transactions files found for today {today_str}.")
    raise FileNotFoundError(f"No transactions files found for today {today_str}.")


spark = (SparkSession.builder 
    .master("spark://spark-master:7077") 
    .appName("bootstrap_stores") 
    .config(conf=conf) 
    .getOrCreate())

transactions_df = (spark.read.format("csv") 
    .option("header", True) 
    .option("sep", ";") 
    .option("comment", "#")  # <-- skip lines starting with #
    .schema(schema)
    .load(transactions_full_path))


#####clean bad values 1999 in date column
# Extract the file name using input_file_name()
transactions_df = transactions_df.withColumn("file_name", F.input_file_name())
# Extract the date part from the file name using regular expression
transactions_df = transactions_df.withColumn(
    "extracted_date",
    F.regexp_extract(F.col("file_name"), r"transactions_(\d{4}-\d{2}-\d{2})_\d{1,2}.csv", 1)
)
# Replace the invalid date with the extracted date
transactions_df = transactions_df.withColumn(
    "date",  F.to_date(F.col("extracted_date"), "yyyy-MM-dd") 
)
# Drop the helper columns
transactions_df = transactions_df.drop("file_name", "extracted_date")



####Create the new datetime_ts column
transactions_df = transactions_df.withColumn(
    "datetime_ts",
    F.to_timestamp(
        F.concat_ws(
            " ", 
            F.col("date").cast("string"), 
            F.format_string("%02d:%02d:00", F.col("hour"), F.col("minute"))
        ),
        "yyyy-MM-dd HH:mm:ss"
    )
)
#Drop the now redundant 'minutes' columns
transactions_df = transactions_df.drop(F.col("minute"))

##### load clients table from iceberg
table_clients_path = "hive_prod.db.clients"
try:
    # Load the Iceberg table
    clients_df = spark.read.format("iceberg").load(table_clients_path)
     # Select only 'id' and 'account_id' columns
    clients_df = clients_df.select(F.col("id"), F.col("account_id"))
    clients_df = clients_df.withColumnRenamed("id", "client_id")
except AnalysisException as e:
    # If the table does not exist, raise an error
    if "TableNotFoundException" in str(e):
        raise ValueError(f"Table {table_clients_path} does not exist!")
    else:
        raise e

######handle duplicates for client_id
duplicates_exist = clients_df.groupBy("client_id").count().filter("count > 1").limit(1).count() > 0
if duplicates_exist:
    logger.warning("WARNING: Duplicated client IDs detected in clients_df! You may have duplicated transactions")
else:
    logger.info(" No duplicates detected in clients table based on client_id.")

####do the join with the clients table to get account_id
transactions_df = transactions_df.join(
    F.broadcast(clients_df),
    on="client_id",
    how="left"
)


#####detect unknown clients
unknown_clients_df = transactions_df.filter(F.col("account_id").isNull())
if unknown_clients_df.count() > 0:
    logger.error(f"Warning {unknown_clients_df.count()} transactions avec des client_id inconnus détectées !")
    # Option : lever une exception pour arrêter l'exécution
    raise Exception("Client IDs inconnus trouvés dans transactions. Vérifiez vos données clients !")



#####Ajouter ingestion_ts + extraire les colonnes de partition date_yyyy_mm_dd et hour pour les besoins d'audit
# Calculate current timestamp once

transactions_df = transactions_df.withColumn("ingestion_ts", current_timestamp())








#### # #  Append new data

transactions_df.writeTo("hive_prod.db.transactions").append()

#le overwrite dynamic ne fonctionne pas non plus
# Overwrite the partition for 'date' = '2023-01-01'
# (transactions_df.write 
#     .format("iceberg") 
#     .mode("overwrite") 
#     .option("partitionOverwriteMode", "dynamic") 
#     .partitionBy("date","hour") 
#     .save("hive_prod.db.transactions"))


#le replacement de la partition ne fonctionne pas non plus
# (transactions_df.write 
#     .format("iceberg") 
#     .mode("overwrite") 
#     .option("replaceWhere", f"date = '{today_str}'") 
#     .save("hive_prod.db.transactions"))

### merge into
#the solution of merge into which should work but is not working for me
#transactions_df.createOrReplaceTempView("new_transactions_temp_view")
# spark.sql("""
#     MERGE INTO hive_prod.db.transactions t
#     USING new_transactions_temp_view s
#     ON t.transaction_id = s.transaction_id
#     WHEN MATCHED THEN UPDATE SET *
#     WHEN NOT MATCHED THEN INSERT *
# """)
#error 
#j'ai explore l'hisotire de ram , l'histoire de current_timestamp et ca marche pas
#*** pyspark.errors.exceptions.captured.AnalysisException: [RECURSIVE_VIEW] Recursive view `new_transactions_temp_view` detected (cycle: `new_transactions_temp_view` -> `new_transactions_temp_view`).


#### dedeplucation
#the solution to later deduplicate the table is exploding the memory
# df = spark.read.format("iceberg").load("hive_prod.db.transactions")
# # Deduplicate
# df = df.repartition(100, "date")
# df_dedup = df.dropDuplicates()
# # Optionally overwrite the table
# df_dedup.write.format("iceberg").mode("overwrite").save("hive_prod.db.transactions")




