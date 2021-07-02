import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from resourcesflattener import ResourcesFlattener

spark = SparkSession.builder.getOrCreate()

class load_test_data:
    path = 0
    df = 0

    def load_test_data(self, file_name):
        self.path = '/caa-streamworx/caaswx/spark/data/'
        self.path = self.path + file_name
        self.df = spark.read.parquet(self.path)
        return self.df
