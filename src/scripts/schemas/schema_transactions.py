from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType


schema_transactions = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("client_id", StringType(), False),
    StructField("date", DateType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("store_id", StringType(), False)
])

