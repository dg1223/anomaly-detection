
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, ArrayType, TimestampType, MapType
from pyspark.sql.functions import to_timestamp, col, shuffle, rand

from agentstringflattener import UserAgentFlattenerParser

sc = SparkContext('local')
spark = SparkSession(sc)

test_schema = StructType([ 
        StructField('SM_CLIENTIP', StringType()),
        StructField('SM_AGENTNAME', StringType()),
        StructField('SM_TIMESTAMP', TimestampType())
    ])

ans_schema = StructType([ 
        StructField('SM_CLIENTIP', StringType()),
        StructField('window_temp', StructType([
                StructField('start', TimestampType()),
                StructField('end', TimestampType())
            ])),
        StructField('SM_AGENTNAME', StringType())
        
    ])


class agentflattener_datasets:

    def ds1_base(self):
        test_1_data = [
            ('User_A', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84', '2018-01-01T18:32:26.000+0000'),
            ('User_A', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36', '2018-01-01T18:32:26.000+0000'),
            ('User_A', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84', '2018-01-01T18:32:26.000+0000'),
            ('User_A', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36', '2018-01-01T18:32:26.000+0000'),
        ]

        ans_1_data = [
            ('User_A', 
            [{'platform': {'name': 'Mac OS', 'version': 'X 10.12.6'}, 'os': {'name': 'Macintosh'}, 'bot': False, 'flavor': {'name': 'MacOS', 'version': 'X 10.12.6'}, 'browser': {'name': 'Chrome', 'version': '63.0.3239.84'}}],
            {"start":"2018-01-01T18:30:00.000+0000","end":"2018-01-01T18:45:00.000+0000"}
            )
        ]

        test_df = spark.createDataFrame(test_1_data, schema=test_schema)
        ans_df = spark.createDataFrame(ans_1_data, schema=ans_schema)

        result = UserAgentFlattenerParser(agentSizeLimit=2, entityName = 'SM_CLIENTIP').transform(test_df)
        
        return result, ans_df
        