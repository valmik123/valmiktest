from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\zips.json"
df=spark.read.format("json").option("mode","DROPMALFORMED").load(data)
df=df.withColumnRenamed("_id","id").withColumnRenamed("pop","sal")\
    .withColumn("id",col("id").cast(IntegerType()))\
    .withColumn("lang",col("loc")[0])\
    .withColumn("lati",col("loc")[1]).drop("loc")

#data cleaning completed
# process data
res=df.groupBy(col("state")).agg(count("*")).alias("cnt")
res.show()
# .withColumn("loc", explode(col("loc")))
#rename one column ...use withColumnRenamed
#explode... if column in array format unnest data make line by line
df.show()
#res.write.format("jdbc")
df.printSchema()