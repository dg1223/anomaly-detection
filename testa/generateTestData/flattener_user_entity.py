from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
#from resourceflattener import ResourcesFlattener
from pyspark.sql.types import StructType,StructField, StringType, ArrayType, TimestampType, MapType
from pyspark.sql.functions import to_timestamp, col, shuffle, rand

sc = SparkContext('local')
spark = SparkSession(sc)

test_user_schema = StructType([
            StructField('SM_USERNAME', StringType()),
            StructField('SM_RESOURCE', StringType()),
            StructField('SM_TIMESTAMP_TEMP', StringType())
        ])

ans_schema = StructType([
            StructField('SM_USERNAME', StringType()),
            StructField('window_temp', StructType([
                StructField('start', StringType()),
                StructField('end', StringType())
            ])),
            StructField('SM_RESOURCE', ArrayType(StringType()))
        ])

class Test_datasets:

    def dataset_one_window_10(self):
        test_data_one_window_case = [
            ('User_A', 'Resource_A', '2018-02-01T00:00:00.000+0000'),
            ('User_A', 'Resource_B', '2018-02-01T00:01:00.000+0000'),
            ('User_A', 'Resource_C', '2018-02-01T00:02:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:01:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:02:00.000+0000'),
            ('User_B', 'Resource_C', '2018-02-01T00:03:00.000+0000'),
            ('User_B', 'Resource_D', '2018-02-01T00:04:00.000+0000'),
            ('User_B', 'Resource_E', '2018-02-01T00:05:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:06:00.000+0000'),
            ('User_B', 'Resource_D', '2018-02-01T00:07:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:08:00.000+0000'),
            ('User_B', 'Resource_E', '2018-02-01T00:09:00.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:02:00.000+0000'),
            ('User_C', 'Resource_C', '2018-02-01T00:05:00.000+0000'),
            ('User_C', 'Resource_B', '2018-02-01T00:05:01.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:05:02.000+0000'),
            ('User_C', 'Resource_D', '2018-02-01T00:07:00.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:06:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:08:00.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:08:01.000+0000'),
            ('User_D', 'Resource_D', '2018-02-01T00:08:04.000+0000'),
            ('User_D', 'Resource_E', '2018-02-01T00:09:00.000+0000'),
            ('User_D', 'Resource_F', '2018-02-01T00:09:03.000+0000'),
            ('User_D', 'Resource_G', '2018-02-01T00:11:00.000+0000'),
            ('User_D', 'Resource_H', '2018-02-01T00:12:00.000+0000'),
            ('User_D', 'Resource_I', '2018-02-01T00:13:00.000+0000'),
            ('User_D', 'Resource_J', '2018-02-01T00:14:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:14:01.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:14:03.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:14:08.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:14:09.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:14:40.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:14:50.000+0000'),
            ('User_E', 'Resource_A', '2018-02-01T00:01:00.000+0000'),
            ('User_E', 'Resource_C', '2018-02-01T00:09:00.000+0000'),

        ]
        test_df = spark.createDataFrame(test_data_one_window_case, schema=test_user_schema)
        test_df = test_df.withColumn('SM_TIMESTAMP', col('SM_TIMESTAMP_TEMP').cast('timestamp'))
        test_df = test_df.drop('SM_TIMESTAMP_TEMP')

        flattener = ResourcesFlattener(maxResourceCount=10)

        result = flattener.transform(test_df)

        ans_data_10 = [

            ('User_D', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_E", "Resource_F", "Resource_G",
              "Resource_H", \
              "Resource_I", "Resource_J"]),
            ('User_C', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_C", "Resource_B", "Resource_A", "Resource_D"]),
            ('User_A', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C"]),
            ('User_B', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_E", "Resource_B", "Resource_D",
              "Resource_A", "Resource_E"]),
            ('User_E', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_C"]),
        ]

        ans_df_10 = spark.createDataFrame(ans_data_10, schema=ans_schema)
        ans_df_10 = ans_df_10.withColumn('window', col('window_temp').cast(
            StructType([StructField('start', TimestampType()), StructField('end', TimestampType())])))
        ans_df_10 = ans_df_10.drop('window_temp')
        ans_df_10 = ans_df_10.select('SM_USERNAME', 'window', 'SM_RESOURCE')

        return result, ans_df_10

    def dataset_multiple_windows_5(self):
        test_data_multiple_window_case = [

            ('User_A', 'Resource_A', '2018-02-01T00:00:00.000+0000'),
            ('User_A', 'Resource_B', '2018-02-01T00:01:00.000+0000'),
            ('User_A', 'Resource_C', '2018-02-01T02:00:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:01:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:02:00.000+0000'),
            ('User_B', 'Resource_C', '2018-02-01T00:03:00.000+0000'),
            ('User_B', 'Resource_D', '2018-02-01T00:04:00.000+0000'),
            ('User_B', 'Resource_E', '2018-02-01T00:05:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:06:00.000+0000'),
            ('User_B', 'Resource_D', '2018-02-01T00:20:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:20:00.000+0000'),
            ('User_B', 'Resource_E', '2018-02-01T00:20:00.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:02:00.000+0000'),
            ('User_C', 'Resource_C', '2018-02-01T00:05:00.000+0000'),
            ('User_C', 'Resource_B', '2018-02-01T00:20:01.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:20:02.000+0000'),
            ('User_C', 'Resource_D', '2018-02-01T00:20:00.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:06:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:08:00.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:08:01.000+0000'),
            ('User_D', 'Resource_D', '2018-02-01T00:08:04.000+0000'),
            ('User_D', 'Resource_E', '2018-02-01T00:09:00.000+0000'),
            ('User_D', 'Resource_F', '2018-02-01T00:09:03.000+0000'),
            ('User_D', 'Resource_G', '2018-02-01T00:11:00.000+0000'),
            ('User_D', 'Resource_H', '2018-02-01T00:12:00.000+0000'),
            ('User_D', 'Resource_I', '2018-02-01T00:13:00.000+0000'),
            ('User_D', 'Resource_J', '2018-02-01T01:14:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T01:14:01.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T01:14:03.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T01:14:08.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T01:14:09.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T01:14:40.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T01:14:50.000+0000'),
            ('User_E', 'Resource_A', '2018-02-01T00:01:00.000+0000'),
            ('User_E', 'Resource_C', '2018-02-01T01:09:00.000+0000'),

        ]

        test_df = spark.createDataFrame(test_data_multiple_window_case, schema=test_user_schema)
        test_df = test_df.withColumn('SM_TIMESTAMP', col('SM_TIMESTAMP_TEMP').cast('timestamp'))
        test_df = test_df.drop('SM_TIMESTAMP_TEMP')

        flattener = ResourcesFlattener(maxResourceCount=5)
        result = flattener.transform(test_df)

        ans_df_5 = [

            ('User_A', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B"]),
            (
            'User_A', {"start": "2018-02-01T02:00:00.000+0000", "end": "2018-02-01T02:15:00.000+0000"}, ["Resource_C"]),
            ('User_B', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_E"]),
            ('User_B', {"start": "2018-02-01T00:15:00.000+0000", "end": "2018-02-01T00:30:00.000+0000"},
             ["Resource_D", "Resource_A", "Resource_E"]),
            ('User_C', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_C"]),
            ('User_C', {"start": "2018-02-01T00:15:00.000+0000", "end": "2018-02-01T00:30:00.000+0000"},
             ["Resource_D", "Resource_B", "Resource_A"]),
            ('User_D', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_E"]),
            ('User_D', {"start": "2018-02-01T01:00:00.000+0000", "end": "2018-02-01T01:15:00.000+0000"},
             ["Resource_J", "Resource_B", "Resource_C", "Resource_A", "Resource_B"]),
            (
            'User_E', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"}, ["Resource_A"]),
            ('User_E', {"start": "2018-02-01T01:00:00.000+0000", "end": "2018-02-01T01:15:00.000+0000"}, ["Resource_C"])

        ]

        ans_df_5 = spark.createDataFrame(ans_df_5, schema=ans_schema)
        ans_df_5 = ans_df_5.withColumn('window', col('window_temp').cast(
            StructType([StructField('start', TimestampType()), StructField('end', TimestampType())])))
        ans_df_5 = ans_df_5.drop('window_temp')
        ans_df_5 = ans_df_5.select('SM_USERNAME', 'window', 'SM_RESOURCE')

        return result, ans_df_5

    def dataset_single_window_duplicates(self):
        test_duplicate_resources_same_window = [

            ('User_A', 'Resource_A', '2018-02-01T00:00:00.000+0000'),
            ('User_A', 'Resource_B', '2018-02-01T00:01:00.000+0000'),
            ('User_A', 'Resource_B', '2018-02-01T00:02:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:01:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:02:00.000+0000'),
            ('User_B', 'Resource_C', '2018-02-01T00:03:00.000+0000'),
            ('User_B', 'Resource_C', '2018-02-01T00:04:00.000+0000'),
            ('User_B', 'Resource_C', '2018-02-01T00:05:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:06:00.000+0000'),
            ('User_B', 'Resource_D', '2018-02-01T00:07:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:08:00.000+0000'),
            ('User_B', 'Resource_E', '2018-02-01T00:09:00.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:02:00.000+0000'),
            ('User_C', 'Resource_B', '2018-02-01T00:05:00.000+0000'),
            ('User_C', 'Resource_B', '2018-02-01T00:05:01.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:05:02.000+0000'),
            ('User_C', 'Resource_D', '2018-02-01T00:07:00.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:06:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:08:00.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:08:01.000+0000'),
            ('User_D', 'Resource_D', '2018-02-01T00:08:04.000+0000'),
            ('User_D', 'Resource_D', '2018-02-01T00:09:00.000+0000'),
            ('User_D', 'Resource_F', '2018-02-01T00:09:03.000+0000'),
            ('User_D', 'Resource_G', '2018-02-01T00:11:00.000+0000'),
            ('User_D', 'Resource_H', '2018-02-01T00:12:00.000+0000'),
            ('User_D', 'Resource_I', '2018-02-01T00:13:00.000+0000'),
            ('User_D', 'Resource_J', '2018-02-01T00:14:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:14:01.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:14:03.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:14:08.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:14:09.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:14:40.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:14:50.000+0000'),
            ('User_E', 'Resource_A', '2018-02-01T00:01:00.000+0000'),
            ('User_E', 'Resource_A', '2018-02-01T00:09:00.000+0000'),

        ]

        test_df = spark.createDataFrame(test_duplicate_resources_same_window, schema=test_user_schema)
        test_df = test_df.withColumn('SM_TIMESTAMP', col('SM_TIMESTAMP_TEMP').cast('timestamp'))
        test_df = test_df.drop('SM_TIMESTAMP_TEMP')

        flattener = ResourcesFlattener(maxResourceCount=5)
        result = flattener.transform(test_df)

        ans_data_5 = [

            ('User_A', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_B"]),
            ('User_B', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C", "Resource_C", "Resource_C"]),
            ('User_C', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_B", "Resource_A", "Resource_D"]),
            ('User_D', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_D"]),
            ('User_E', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_A"]),
        ]

        ans_df_5 = spark.createDataFrame(ans_data_5, schema=ans_schema)
        ans_df_5 = ans_df_5.withColumn('window', col('window_temp').cast(
            StructType([StructField('start', TimestampType()), StructField('end', TimestampType())])))
        ans_df_5 = ans_df_5.drop('window_temp')
        ans_df_5 = ans_df_5.select('SM_USERNAME', 'window', 'SM_RESOURCE')

        return result, ans_df_5

    def dataset_single_window_duplicate_rows(self):
        test_duplicate_rows = [

            ('User_A', 'Resource_A', '2018-02-01T00:00:00.000+0000'),
            ('User_A', 'Resource_B', '2018-02-01T00:01:00.000+0000'),
            ('User_A', 'Resource_B', '2018-02-01T00:01:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:01:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:02:00.000+0000'),
            ('User_B', 'Resource_C', '2018-02-01T00:03:00.000+0000'),
            ('User_B', 'Resource_C', '2018-02-01T00:03:00.000+0000'),
            ('User_B', 'Resource_C', '2018-02-01T00:03:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:06:00.000+0000'),
            ('User_B', 'Resource_D', '2018-02-01T00:07:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:08:00.000+0000'),
            ('User_B', 'Resource_E', '2018-02-01T00:09:00.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:02:00.000+0000'),
            ('User_C', 'Resource_B', '2018-02-01T00:05:00.000+0000'),
            ('User_C', 'Resource_B', '2018-02-01T00:05:00.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:05:02.000+0000'),
            ('User_C', 'Resource_D', '2018-02-01T00:07:00.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:06:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:08:00.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:08:01.000+0000'),
            ('User_D', 'Resource_D', '2018-02-01T00:08:04.000+0000'),
            ('User_D', 'Resource_D', '2018-02-01T00:09:00.000+0000'),
            ('User_D', 'Resource_F', '2018-02-01T00:09:03.000+0000'),
            ('User_D', 'Resource_G', '2018-02-01T00:11:00.000+0000'),
            ('User_D', 'Resource_H', '2018-02-01T00:12:00.000+0000'),
            ('User_D', 'Resource_I', '2018-02-01T00:13:00.000+0000'),
            ('User_D', 'Resource_J', '2018-02-01T00:14:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:14:01.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:14:03.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:06:00.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:06:00.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:06:00.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:06:00.000+0000'),
            ('User_E', 'Resource_A', '2018-02-01T00:01:00.000+0000'),
            ('User_E', 'Resource_A', '2018-02-01T00:01:00.000+0000'),

        ]

        test_df = spark.createDataFrame(test_duplicate_rows, schema=test_user_schema)
        test_df = test_df.withColumn('SM_TIMESTAMP', col('SM_TIMESTAMP_TEMP').cast('timestamp'))
        test_df = test_df.drop('SM_TIMESTAMP_TEMP')

        flattener = ResourcesFlattener(maxResourceCount=5)
        result = flattener.transform(test_df)

        ans_data_5 = [

            ('User_A', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_B"]),
            ('User_B', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C", "Resource_C", "Resource_C", "Resource_B", "Resource_D"]),
            ('User_C', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_B", "Resource_A", "Resource_D"]),
            ('User_D', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_A", "Resource_A", "Resource_A", "Resource_A", "Resource_B", "Resource_C",
              "Resource_D", "Resource_D"]),
            ('User_E', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_A"]),
        ]
        ans_df_5 = spark.createDataFrame(ans_data_5, schema=ans_schema)
        ans_df_5 = ans_df_5.withColumn('window', col('window_temp').cast(
            StructType([StructField('start', TimestampType()), StructField('end', TimestampType())])))
        ans_df_5 = ans_df_5.drop('window_temp')
        ans_df_5 = ans_df_5.select('SM_USERNAME', 'window', 'SM_RESOURCE')

        return result, ans_df_5

    def dataset_check_user_based_grouping(self):
        test_user_based_grouping = [

            ('User_A', 'Resource_A', '2018-02-01T00:00:00.000+0000'),
            ('User_A', 'Resource_B', '2018-02-01T00:01:00.000+0000'),
            ('User_A', 'Resource_C', '2018-02-01T00:02:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:00:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:01:00.000+0000'),
            ('User_B', 'Resource_C', '2018-02-01T00:02:00.000+0000'),
            ('User_B', 'Resource_D', '2018-02-01T00:04:00.000+0000'),
            ('User_B', 'Resource_E', '2018-02-01T00:05:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:06:00.000+0000'),
            ('User_B', 'Resource_D', '2018-02-01T00:07:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:08:00.000+0000'),
            ('User_B', 'Resource_E', '2018-02-01T00:09:00.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:00:00.000+0000'),
            ('User_C', 'Resource_B', '2018-02-01T00:01:00.000+0000'),
            ('User_C', 'Resource_C', '2018-02-01T00:02:01.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:05:02.000+0000'),
            ('User_C', 'Resource_D', '2018-02-01T00:07:00.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:00:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:01:00.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:02:01.000+0000'),
            ('User_D', 'Resource_D', '2018-02-01T00:08:04.000+0000'),
            ('User_D', 'Resource_E', '2018-02-01T00:09:00.000+0000'),
            ('User_D', 'Resource_F', '2018-02-01T00:09:03.000+0000'),
            ('User_D', 'Resource_G', '2018-02-01T00:11:00.000+0000'),
            ('User_D', 'Resource_H', '2018-02-01T00:12:00.000+0000'),
            ('User_D', 'Resource_I', '2018-02-01T00:13:00.000+0000'),
            ('User_D', 'Resource_J', '2018-02-01T00:14:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:14:01.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:14:03.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:14:08.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:14:09.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:14:40.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:14:50.000+0000'),
            ('User_E', 'Resource_A', '2018-02-01T00:00:00.000+0000'),
            ('User_E', 'Resource_B', '2018-02-01T00:01:00.000+0000'),

        ]

        test_df = spark.createDataFrame(test_user_based_grouping, schema=test_user_schema)
        test_df = test_df.withColumn('SM_TIMESTAMP', col('SM_TIMESTAMP_TEMP').cast('timestamp'))
        test_df = test_df.drop('SM_TIMESTAMP_TEMP')

        flattener = ResourcesFlattener(maxResourceCount=5)
        result = flattener.transform(test_df)
        ans_data_5 = [

            ('User_A', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C"]),
            ('User_B', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_E"]),
            ('User_C', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C", "Resource_A", "Resource_D"]),
            ('User_D', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_E"]),
            ('User_E', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_A", "Resource_B"]),
        ]
        ans_df_5 = spark.createDataFrame(ans_data_5, schema=ans_schema)
        ans_df_5 = ans_df_5.withColumn('window', col('window_temp').cast(
            StructType([StructField('start', TimestampType()), StructField('end', TimestampType())])))
        ans_df_5 = ans_df_5.drop('window_temp')
        ans_df_5 = ans_df_5.select('SM_USERNAME', 'window', 'SM_RESOURCE')

        return result, ans_df_5

    def dataset_shuffled(self):
        test_shuffled_data = [

            ('User_A', 'Resource_A', '2018-02-01T00:10:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:10:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:02:00.000+0000'),
            ('User_A', 'Resource_B', '2018-02-01T00:01:00.000+0000'),
            ('User_A', 'Resource_C', '2018-02-01T00:02:00.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:10:00.000+0000'),
            ('User_C', 'Resource_C', '2018-02-01T00:05:00.000+0000'),
            ('User_B', 'Resource_C', '2018-02-01T00:03:00.000+0000'),
            ('User_B', 'Resource_D', '2018-02-01T00:04:00.000+0000'),
            ('User_B', 'Resource_E', '2018-02-01T00:05:00.000+0000'),
            ('User_B', 'Resource_B', '2018-02-01T00:06:00.000+0000'),
            ('User_B', 'Resource_D', '2018-02-01T00:07:00.000+0000'),
            ('User_B', 'Resource_A', '2018-02-01T00:08:00.000+0000'),
            ('User_B', 'Resource_E', '2018-02-01T00:09:00.000+0000'),
            ('User_C', 'Resource_B', '2018-02-01T00:05:01.000+0000'),
            ('User_C', 'Resource_A', '2018-02-01T00:05:02.000+0000'),
            ('User_C', 'Resource_D', '2018-02-01T00:07:00.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:10:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:08:00.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:08:01.000+0000'),
            ('User_D', 'Resource_D', '2018-02-01T00:08:04.000+0000'),
            ('User_D', 'Resource_E', '2018-02-01T00:09:00.000+0000'),
            ('User_D', 'Resource_F', '2018-02-01T00:09:03.000+0000'),
            ('User_D', 'Resource_G', '2018-02-01T00:11:00.000+0000'),
            ('User_D', 'Resource_H', '2018-02-01T00:12:00.000+0000'),
            ('User_D', 'Resource_I', '2018-02-01T00:13:00.000+0000'),
            ('User_D', 'Resource_J', '2018-02-01T00:14:00.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:14:01.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:14:03.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:14:08.000+0000'),
            ('User_D', 'Resource_B', '2018-02-01T00:14:09.000+0000'),
            ('User_D', 'Resource_C', '2018-02-01T00:14:40.000+0000'),
            ('User_D', 'Resource_A', '2018-02-01T00:14:50.000+0000'),
            ('User_E', 'Resource_A', '2018-02-01T00:10:00.000+0000'),
            ('User_E', 'Resource_C', '2018-02-01T00:09:00.000+0000'),

        ]

        test_df = spark.createDataFrame(test_shuffled_data, schema=test_user_schema)
        test_df = test_df.withColumn('SM_TIMESTAMP', col('SM_TIMESTAMP_TEMP').cast('timestamp'))
        test_df = test_df.drop('SM_TIMESTAMP_TEMP')

        flattener = ResourcesFlattener(maxResourceCount=5)
        result = flattener.transform(test_df)

        ans_data_5 = [

            ('User_A', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_B", "Resource_C", "Resource_A", ]),
            ('User_B', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_B", "Resource_C", "Resource_D", "Resource_E", "Resource_B"]),
            ('User_C', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_C", "Resource_B", "Resource_A", "Resource_D", "Resource_A"]),
            ('User_D', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_B", "Resource_C", "Resource_D", "Resource_E", "Resource_F"]),
            ('User_E', {"start": "2018-02-01T00:00:00.000+0000", "end": "2018-02-01T00:15:00.000+0000"},
             ["Resource_C", "Resource_A"]),
        ]
        ans_df_5 = spark.createDataFrame(ans_data_5, schema=ans_schema)
        ans_df_5 = ans_df_5.withColumn('window', col('window_temp').cast(
            StructType([StructField('start', TimestampType()), StructField('end', TimestampType())])))
        ans_df_5 = ans_df_5.drop('window_temp')
        ans_df_5 = ans_df_5.select('SM_USERNAME', 'window', 'SM_RESOURCE')

        return result, ans_df_5