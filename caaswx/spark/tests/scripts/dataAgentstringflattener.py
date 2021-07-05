"""
Note: not working yet, based off of the main branch's flattener_user_entity.py
the issue is likely with the typing of the dataframes, timestamp seems to give some problems

"""

import httpagentparser

import pandas as pd
import pyspark.sql.functions as f

from pyspark import keyword_only
from pyspark.ml import Transformer, UnaryTransformer
from pyspark.ml.param.shared import TypeConverters, Param, Params

from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.functions import window, col, pandas_udf, PandasUDFType, max, min, udf, lit
from pyspark.sql.types import LongType, DoubleType, StringType, TimestampType, StructType, StructField, DateType, FloatType, MapType


from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from IPython.display import display
import pandas as pd

from pyspark.sql.types import DatetimeConverter, StructType,StructField, StringType, ArrayType, TimestampType, MapType
from pyspark.sql.functions import to_timestamp, col, shuffle, rand

from agentstringflattener import UserAgentFlattenerParser

spark = SparkSession.builder.getOrCreate()

test_schema = StructType([
    StructField('SM_CLIENTIP', StringType()),
    StructField('SM_AGENTNAME', StringType()),
    StructField('SM_TIMESTAMP', StringType())
])




class agentflattener_datasets:

    def ds1_base(self):
        ans_schema = StructType([
            StructField('SM_CLIENTIP', StringType()),
            StructField('windowtmp', StructType([
                StructField('start', StringType()),
                StructField('end', StringType())
            ]), False),
            StructField('Parsed_Agent_String', StringType())
        ])

        test_1_data = [
            ('User_A',
             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84',
             '2018-01-01T18:32:26.000+0000'),
            ('User_A',
             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
             '2018-01-01T18:32:26.000+0000'),
            ('User_A',
             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84',
             '2018-01-01T18:32:26.000+0000'),
            ('User_A',
             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
             '2018-01-01T18:32:26.000+0000'),
        ]

        ans_1_data = [
            ('User_A',
             {"start": "2018-01-01T18:30:00.000+0000",
              "end": "2018-01-01T18:45:00.000+0000"},
             [{'platform': {'name': 'Mac OS', 'version': 'X 10.12.6'}, 'os': {'name': 'Macintosh'}, 'bot': False,
               'flavor': {'name': 'MacOS', 'version': 'X 10.12.6'},
               'browser': {'name': 'Chrome', 'version': '63.0.3239.84'}}]

             )
        ]

        ans_df = spark.createDataFrame(ans_1_data, schema=ans_schema)

        ans_df = ans_df.withColumn('window', f.col('windowtmp').cast(
            StructType([StructField('start', TimestampType()), StructField('end', TimestampType())]))).drop('windowtmp')
        ans_df = ans_df.select(['SM_CLIENTIP', 'window', 'Parsed_Agent_String'])

        test_df = spark.createDataFrame(test_1_data, schema=test_schema)

        result = UserAgentFlattenerParser(agentSizeLimit=2, entityName='SM_CLIENTIP',runParser=False).transform(test_df)

        #         print("result: ")
        #         print(result)
        #         print("ans: ")
        #         print(ans_df)

        return result, ans_df

    def ds2_base(self):
        ans_schema = StructType([
            StructField('SM_CLIENTIP', StringType()),
            StructField('windowtmp', StructType([
                StructField('start', StringType()),
                StructField('end', StringType())
            ]), False),
            StructField('SM_AGENTNAME', ArrayType(
                StringType(), False
            ), False)
        ])

        test_1_data = [
            ('User_A',
             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84',
             '2018-01-01T18:32:26.000+0000'),
            ('User_A',
             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
             '2018-01-01T18:32:26.000+0000'),
            ('User_A',
             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84',
             '2018-01-01T18:32:26.000+0000'),
            ('User_A',
             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
             '2018-01-01T18:32:26.000+0000'),
        ]

        ans_1_data = [
            ('User_A',
             {"start": "2018-01-01T18:30:00.000+0000",
              "end": "2018-01-01T18:45:00.000+0000"},
             [
                 "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84",
                 "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36"]

             )
        ]

        test_df = spark.createDataFrame(test_1_data, schema=test_schema)
        ans_df = spark.createDataFrame(ans_1_data, schema=ans_schema)

        ans_df = ans_df.withColumn('window', f.col('windowtmp').cast(
            StructType([StructField('start', TimestampType()), StructField('end', TimestampType())]))).drop('windowtmp')
        ans_df = ans_df.select(['SM_CLIENTIP', 'window', 'SM_AGENTNAME'])
        result = UserAgentFlattenerParser(agentSizeLimit=2, entityName='SM_CLIENTIP', runParser=False).transform(
            test_df)

        #         print("result: ")
        #         print(result)
        #         print("ans: ")
        #         print(ans_df)

        return result, ans_df

# data_importer = agentflattener_datasets()
# result, ans_1_data = data_importer.ds1_base()
# # result.write.parquet("./people.parquet")
# # result.coalesce(1).write.csv("./myresults.csv")
# result.show()
# # print(httpagentparser.detect("something"))