import os

from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()


def write_parquet(filePath: str, schema, data):
    dataFrame = spark.createDataFrame(data, schema=schema)
    dataFrame.write.parquet(filePath)
    return dataFrame


def load_parquet(*argv: str):
    cwd = os.getcwd()
    path = os.path.join(cwd, *argv)
    return spark.read.parquet(path)
