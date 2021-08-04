import os
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()
#
# from pyspark.shell import spark


def load_test_data(folder, file_name):
    """load test parquet_data from parquet_data by passing file name"""
    cwd = os.getcwd()
    path = os.path.join(cwd, folder, file_name)
    df = spark.read.parquet(path)
    return df
