from pyspark.sql.types import StructType, StructField, StringType


#je prefere string pour id et ean si jamais il existe des 00 au debut comme le cas de sirene
#je considere que l'id et l'ean ne peuvent pas etre null
schema_products = StructType([      
    StructField("id", StringType(), False),
    StructField("ean", StringType(), False),
    StructField("brand", StringType(), True),
    StructField("description", StringType(), True)
])