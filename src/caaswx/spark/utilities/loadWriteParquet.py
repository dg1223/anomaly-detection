import os

from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()


def write_parquet(file_path: str, schema, data):
    data_frame = spark.createDataFrame(data, schema=schema)
    data_frame.write.parquet(file_path)
    return data_frame


def load_parquet(*argv: str):
    cwd = os.getcwd()
    path = os.path.join(cwd, *argv)
    return spark.read.parquet(path)
