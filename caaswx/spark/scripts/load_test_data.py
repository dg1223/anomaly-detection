from pyspark.sql.session import SparkSession
spark = SparkSession.builder.getOrCreate()


class LoadTestData:
    """contains method to load parquet files"""
    path = 0
    df = 0

    def load_test_data(self, file_name):
        """ load test data from data by passing file name"""
        self.path = '/caa-streamworx/caaswx/spark/data/'
        self.path = self.path + file_name
        self.df = spark.read.parquet(self.path)
        return self.df
