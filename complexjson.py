from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\world_bank.json"
df=spark.read.format("json").option("mode","DROPMALFORMED").load(data)
#df.show(truncate=False)
#df.printSchema()
res=df.withColumn("theme_namecode", explode(col("theme_namecode"))).withColumn("theme_namecode_code",col("theme_namecode.code")).withColumn("theme_namecode_name",col("theme_namecode.name"))\
    .withColumn("theme1_name",col("theme1.name"))\
    .withColumn("theme1_Percent",col("theme1.Percent")).drop("theme1")\
    .withColumn("sector_namecode",explode("sector_namecode"))\
    .withColumn("sector_namecode_code",col("sector_namecode.code"))\
    .withColumn("sector_namecode_name",col("sector_namecode.name")).drop("sector_namecode")\
    .withColumn("sector", explode(col("sector"))).withColumn("sector_name",col("sector.name")).drop("sector")\
    .withColumn("sector1_Name",col("sector1.Name"))\
    .withColumn("sector1_Percent",col("sector1.Percent")).drop("sector1")\
    .withColumn("sector2_Name",col("sector2.Name"))\
    .withColumn("sector2_Percent",col("sector2.Percent")).drop("sector2")\
    .withColumn("sector3_Name",col("sector3.Name"))\
    .withColumn("sector3_Percent",col("sector3.Percent")).drop("sector3")\
    .withColumn("sector4_Name",col("sector4.Name"))\
    .withColumn("sector4_Percent",col("sector4.Percent")).drop("sector4")
res.show()
res.printSchema()
#if u want to remove array .. use explode(array_column)
#if u want remove struct .. use parentColumn.ChildColumn
#struct datatype type - usually collection of multiple datatype of the elements...{8, Human development}, {11, }
#array ... [Human development] - collection of same data type elements.
