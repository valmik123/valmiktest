from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\flights.txt"
df=spark.read.format("csv").option("header","true").option("interSchema","true").load(data)
#df.show()
res=df.withColumn("dest", explode(array_distinct(split(col("dest"),"-")))).groupBy(col("name")).agg(count("dest").alias("cnt"))
res.show()
res.printSchema()
#print datatype in nice tree format use printSchema()

#if u use split it split based on sep ... result u ll get in array ...
#if u use explode ... remove array and get everything in line by line