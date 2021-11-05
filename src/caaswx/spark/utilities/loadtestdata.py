import os
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()


#
# from pyspark.shell import spark


def load_test_data(*args):
    """load test parquet_data from parquet_data by passing file name"""
    cwd = os.getcwd()
    path = os.path.join(cwd, *args)
    df = spark.read.parquet(path)
    return df


def load_path(*args):
    """load the path of any file for testing"""
    cwd = os.getcwd()
    path = os.path.join(cwd, *args)
    return path
