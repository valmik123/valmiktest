from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data = "C:\\bigdata\\drivers\\asl.csv"
df = spark.read.format("csv").option("header","true").option("inferSchema",'true').load(data)
#df = spark.read.format("csv").option("header","false").load(data)
#df.show()
#df.printSchema()
df.createOrReplaceTempView("tab")
#res=spark.sql("select *from tab where age > 30")
#res=spark.sql("select city,count(*) cnt from tab group by city order by cnt desc")
#res = df.where(col("city")=="hyd")
#res = df.where(col("age")>=30)
res = df.where((col("age")>=30) & (col("city")!="hyd"))
res.show()