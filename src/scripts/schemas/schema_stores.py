from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#directly reading latlng as a tuple during CSV load is not possible natively with Spark.
#Spark’s CSV reader always reads fields as simple types (StringType, IntegerType, etc.)
# because CSV is a "flat" format — it doesn’t support complex types like tuples
#  (StructType, ArrayType) inside a column at read time.
schema_stores = StructType([
    StructField("id", StringType(), False),
    StructField("latlng", StringType(), True),
    StructField("opening", IntegerType(), True),
    StructField("closing", IntegerType(), True),
    StructField("type", IntegerType(), True)
])
