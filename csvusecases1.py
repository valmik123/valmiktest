from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
                                     "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
#data="C:\\bigdata\\drivers\\bank-full.csv"
data="s3a://valmik/input/bank-full.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",";").option("mode","DROPMALFORMED").load(data)
#df.show()
df.createOrReplaceTempView("temp")
#res=spark.sql("select * from temp where age>60 and balance>50000 and marital!='married'")
#res=df.where((col("age")>60) & (col("marital")!="divorced") & (col("balance")>=50000))
res=df.where((col("age")>60) & (col("marital").isin("divorced",'married')) & (col("balance")>=50000))
res.show()
res.write.mode("overwrite").format("csv").option("header","true").save("s3a://valmik2023/result/bankdata")