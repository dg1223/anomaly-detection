# from pyspark.context import SparkContext
# from pyspark.sql.session import SparkSession
#
# sc = SparkContext("local")
# spark = SparkSession(sc)


def writeParquet(path: str, fileName: str, schema, data, spark):
    filePath = path + fileName
    dataFrame = spark.createDataFrame(data, schema=schema)
    dataFrame.write.parquet(filePath)
    return dataFrame


def loadParquet(path: str, spark):
    return spark.read.parquet(path)
