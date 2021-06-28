from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

 

spark = SparkSession.builder.appName('demo').getOrCreate()

 

df = spark.table('diamonds')
df.show(5)