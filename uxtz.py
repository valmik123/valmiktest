from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\datasets\\CSV\\CSV\\ethereum_dataset.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df.show()
def sec2dt(sec):
    min = int(sec/60)
    hrs = int(min/3600)
    days = int(hrs/24)
    yrs = days // 365
    mns = (days - yrs * 365) // 30
    result = yrs, "years", mns, "months", days, "days",hrs, "hours", min, "minutes"
    return result
#python function convert to udf
udfunc = udf(sec2dt)

res=df.select("Date(UTC)","UnixTimeStamp").withColumn("iuxt",unix_timestamp())\
    .withColumn("utx2ist",from_utc_timestamp(from_unixtime(col("UnixTimeStamp")),"IST"))\
    .withColumn("isttime",current_timestamp())\
    .withColumn("diff",col("isttime").cast("long")-col("utx2ist").cast("long"))\
    .withColumn("minus",col("isttime")-col("utx2ist"))\
    .withColumn("task",udfunc(col("diff")))
# 5 years, 4 months, 5hours, 20 minutes 10 seconds
#how many seconds completed ....diff
#2015-07-30 05:30:00

res.show(truncate=False)