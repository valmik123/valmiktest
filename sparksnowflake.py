from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sfOptions = {
  "sfURL" : "https://app.snowflake.com",
  "sfUser" : "valmik khairnar",
    "sfPassword" : "Valmik@321",
  "sfDatabase" : "sfspark",
  "sfSchema" : "public",
  "sfWarehouse" : "SQL_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("query", "select *from asl") \
    .load()

df.show()
