from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()
#
# from pyspark.shell import spark

class LoadTestData:
    """contains method to load parquet files"""

    path = 0
    df = 0

    def load_test_data(self, file_name):
        """ load test parquet_data from parquet_data by passing file name"""
        self.path = "/caa-streamworx/data/parquet_data/"
        self.path = self.path + file_name
        self.df = spark.read.parquet(self.path)
        return self.df
