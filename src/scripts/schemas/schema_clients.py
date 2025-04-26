from pyspark.sql.types import StructType, StructField, StringType


schema_clients = StructType([      
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("job", StringType(), True),
    StructField("email", StringType(), True),
    StructField("account_id", StringType(), True)
])