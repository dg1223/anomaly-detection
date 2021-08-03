from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()


def writeParquet(path: str, fileName: str, schema, data):
    filePath = path + fileName
    dataFrame = spark.createDataFrame(data, schema=schema)
    dataFrame.write.parquet(filePath)
    return dataFrame


def loadParquet(path: str, spark):
    return spark.read.parquet(path)
