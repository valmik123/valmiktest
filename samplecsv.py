from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\asl.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df.show()
#df.printSchema()
#process data... sql or python
df.createOrReplaceTempView("tab")
# it allows run sql queries on top of dataframe
#res=spark.sql("select * from tab where age>30")
#res=spark.sql("select city, count(*) cnt from tab group by city order by cnt desc")

#python friendly
#res=df.where(col("city")=="hyd")
#res=df.where(col("age")>=30)
res=df.where((col("age")>=30) & (col("city")!="hyd"))
res.show(5)


#header firstline consider as column ... use header true
# by default spark considered as all columns as string... if u mention inferScema, true... its consider as appropriate datatypes.
