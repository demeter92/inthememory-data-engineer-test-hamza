
import ipdb
from config.spark_config import conf
from pyspark.sql import SparkSession

## Start Spark Session
spark_session = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("test_spark_with_iceberg") \
    .config(conf=conf) \
    .getOrCreate()

## Create a Table (make sure to use table actually in catalog)
spark_session.sql("CREATE DATABASE IF NOT EXISTS hive_prod.db")
spark_session.sql("SHOW TABLES IN hive_prod.db").show()
spark_session.sql("""
    CREATE TABLE if NOT EXISTS hive_prod.db.employees (
        id INT,
        role STRING,
        department STRING,
        salary FLOAT,
        region STRING)
    USING iceberg
""")

spark_session.sql("SHOW TABLES IN hive_prod.db").show()

spark_session.sql("""
    INSERT INTO hive_prod.db.employees VALUES
    (1, 'Engineer', 'IT', 70000.0, 'Europe'),
    (2, 'Manager', 'HR', 80000.0, 'North America'),
    (3, 'Analyst', 'Finance', 65000.0, 'Asia')
""")

spark_session.sql("SELECT * FROM hive_prod.db.employees").show()
ipdb.set_trace()