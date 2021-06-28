import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from resourceflattener import ResourcesFlattener
from pyspark.sql.types import StructType,StructField, StringType, ArrayType, TimestampType, MapType
from pyspark.sql.functions import date_format, to_timestamp, col, shuffle, rand


class masterData:
"""
Master Class for accessing all testing data
TODO:Incorporate working test data
"""

    def __init__(self):
        #create a spark session that we need
        pass

    def dataForTest1():
        return required_data

    def dataForTest2():
        return newDF
