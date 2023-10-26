from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("interSchema","true").load(data)
all=int(df.count())
#df.show(all, truncate=False)
#df.show()
#by default display top 20 lines and first 20 characters in nice table format... if u want to display first 10 lines use show(10)
#somewhere u got josephine darakjy... ... that email more than 20 characters thats y its showing.... if u want to display all... use truncate=false
#withColumn used to add one new column if that column doesn't exists.
#withColumn used to update existing column if that column already exists.
#lit is one function used to add dummy/default values
#res=df.withColumn("age",lit(18)).withColumn("phone1",regexp_replace(col("phone1"),"-",""))
#above its called dataframe api friendly
#below its called sql friendly(spark.sql)
df.createOrReplaceTempView("tab")
#res=spark.sql("select *, regexp_replace(phone1,'-','')phone from tab").drop("phone1").withColumnRenamed("phone","phone1")
#res=spark.sql("select first_name, last_name, company_name, regexp_replace(state,'OH','Ohie')state, zip, phone2, email, web from tab ")
res=df.withColumn("state",when(col("state")=="NJ","NewJersey").when(col("state")=="OH","Ohie").when(col("state")=="CA","Calif").otherwise(col("state")))\
    .withColumn("email",when(col("email").contains("cox"),"***").otherwise(col("email")))\
    .withColumn("email", regexp_replace(col("email"),"aol","***"))\
    .withColumn("fullname", concat(col("first_name"),lit(" "),col("last_name"),lit(" "),col("state")))\
    .withColumn("fname",concat_ws("-",col("first_name"),col("last_name"),col("state")))\
    .withColumn("zip",lpad(col("zip"),5,"0"))\
    .withColumn("phone", concat(lit("***-***-"),substring(col("phone1"),-4,4)))\
    .withColumn("email1", substring(col("email"),0,5))\
    .withColumn("username", substring_index(col("email"),"@",1))\
    .withColumn("mail", substring_index(col("email"),"@",-1))\
    .withColumn("dom",split(col("mail"),"\.")[1])

#res=df.groupBy(col("state")).agg(count("first_name").alias("cnt"),collect_list(col("first_name")).alias("names")).orderBy(col("cnt").desc())
#res=spark.sql("select state, count(*) cnt , collect_list(first_name) allnames from tab group by state")
#res=spark.sql("select * from tab where zip=(select max(zip) from tab)")
#res=spark.sql("update tab set state='NewYork' where state='ny'")
res1=res.groupBy(col("mail")).agg(count(col("*")).alias("cnt")).orderBy(col("cnt").desc())
res1.show(truncate=False)
