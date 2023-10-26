from pyspark.sql import *
from pyspark.sql.functions import *
import configparser
from configparser import *
conf = ConfigParser()
cred = "C:\\bigdata\\drivers\\credentials.txt"
conf.read(cred)

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#extract data
host=conf.get("mscred","mshost");
msuser=conf.get("mscred","mssqlusername");
mspass = conf.get("mscred","mssqlpassword");
msdri = conf.get("mscred","mssqldriver");

phost=conf.get("mscred","phost1");
puser=conf.get("mscred","psqlusername");
ppass = conf.get("mscred","psqlpassword");
pdri = conf.get("mscred","psqldriver");

#BONUS,DEPT,EMP,SALGRADE,asl,asltab,banktab,bk,livedata,newdata
alltabs = spark.read.format("jdbc").option("url", host).option("user", msuser).option("password", mspass).option("driver", msdri)\
   .option("dbtable","(SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE')t").load()
all = [x[0] for x in alltabs.collect()]
#all = ["DEPT","EMP","SALGRADE"]
for x in all:
    df = spark.read.format("jdbc").option("url", host).option("user", msuser).option("password", mspass).option("dbtable", x).option("driver", msdri).load()
    df.show()
    df.write.mode("append").format("jdbc").option("url",phost).option("user",puser).option("password",ppass).option("driver",pdri).option("dbtable",x).save()



