from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\bank-full.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",";").option("mode","DROPMALFORMED").load(data)
#df.show()
df.createOrReplaceTempView("temp")
#res=spark.sql("select * from temp where age>60 and balance>50000 and marital!='married'")
#res=df.where((col("age")>60) & (col("marital")!="divorced") & (col("balance")>=50000))
res=df.where((col("age")>60) & (col("marital").isin("divorced",'married')) & (col("balance")>=50000))
res.show()