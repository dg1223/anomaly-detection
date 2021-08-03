from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()
#
# from pyspark.shell import spark


def load_test_data(file_name):
    """load test parquet_data from parquet_data by passing file name"""
    path = "./data/parquet_data/"
    path = path + file_name
    df = spark.read.parquet(path)
    return df
