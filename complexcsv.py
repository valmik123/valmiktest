from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").option("mode","DROPMALFORMED").load(data)
#df.show()
#data cleaning
#df.columns return all columns in list ... cols = ["Emp ID","Name Prefix","First Name","Middle Initial"]
import re
#all these columns replace special characters
cols= [re.sub("[^0-9A-Za-z]","",d) for d in df.columns]
df=df.toDF(*cols)
#toDF used to rename to all columns
df.show()



#