from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data = "C:\\bigdata\\drivers\\us-500.csv"
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df = spark.read.format("csv").option("header","false").load(data)
all=int(df.count())
#df.show(all, truncate=False)
#df.show()
# by default top 20 lines in table ... idf want to show the first 10 lines show(10)
# somewhere that email that email more... than 20 chars ...if u want to display to use truncate
#res=df.withColumn("age", lit(18)).withColumn("phone1",regexp_replace(col("phone1"),"-",""))
df.createOrReplaceTempView("tab")
#res=spark.sql("select *, regexp_replace(phone1,'-','')phone1 from tab")
#res=spark.sql("select *, regexp_replace(phone1,'-','')phone from tab").drop("phone1").withColumnRenamed("phone","phone1")
#res=spark.sql("select first_name, last_name, company_name, address, city, county,state , zip, regexp_replace(phone1,'-','') phone1, phone2, email, web from tab")
res=df.withColumn("state",when(col("state")=="NJ","NewJersey").when(col("state")=="OH","ohio").when(col("state")=="CA","Calif").otherwise(col("state")))\
    .withColumn("email9", when(col("email").contains("cox"),"*").otherwise(col("email")))\
    .withColumn("email0", regexp_replace(col("email"),"aol","*"))\
    .withColumn("fullname",concat(col("first_name"),lit(" "),col("last_name"),lit(" "),col("state")))\
    .withColumn("fname",concat_ws("_",col("first_name"),col("last_nam"),col("state")))\
    .withColumn("zip",rpad(col("zip"),5,"0"))\
    .withColumn("phone", concat(lit("*-*-"),substring(col("phone1"),-4,4)))\
    .withColumn("email1", substring(col("email"),0,5))\
    .withColumn("username", substring_index(col("email"),"@",1))\
    .withColumn("mail", substring_index(col("email"),"@",-1))\
    .withColumn("dom",split(col("mail"),"\.")[1])

res.show()
