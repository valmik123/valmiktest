from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "IST")
data=sys.argv(0)
#data="C:\\bigdata\\drivers\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df.show()
df.show()


res=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))\
    .withColumn("today",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("dateminus",col("today") - col("dt"))\
    .withColumn("dtadd", date_add(col("today"),100))\
    .withColumn("dtminus",col("today")-col("dt"))\
    .withColumn("dtsub", date_sub(col("today"),100))\
    .withColumn("lastdate",last_day(col("dt")))\
    .withColumn("dtformat",date_format(col("dt"),"dd/MMM/yyyy/EEE/Q/z"))\
    .withColumn("dttrunc",date_trunc("minute",col("ts")))\
    .withColumn("nextday",next_day(col("today"),"Mon"))\
    .withColumn("lstfri",next_day(date_sub(last_day(col("dt")),7),"Fri"))\
    .withColumn("task",date_format(col("lastdate"),"EEEE"))\
    .withColumn("dyofyr",dayofyear(col("dt")))\
    .withColumn("dyofmon",dayofmonth(col("dt")))\
    .withColumn("dyofweek",dayofweek(col("dt")))\
    .withColumn("weekofyr",weekofyear(col("dt")))\
    .withColumn("ux",unix_timestamp())
#from jan 1 1970 london time .. today date how many seconds completed
#dayofweek ..sun1..mon2..tue3....wed4....thus5....fri6....sat7
res.show(truncate=False)
res.printSchema()


#by default spark consider as date must be yyyy-MM-dd... but this data as  21-02-2021 means dd-MM-yyyy so considered as string.
#now convert string data type to date data type...use to_date()
# 1-3-2021 if ur date like 01-03-2021 at that time ok to_date(col("dt"),"dd-MM-yyyy")) is good
#its recommended use to_date (col("dt"),"d-M-yyyy")