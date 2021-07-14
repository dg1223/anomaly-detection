"""Module for writing parquet files given datasets alongwoth their schema"""
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    TimestampType,
)
from pyspark.sql.functions import col

sc = SparkContext("local")
spark = SparkSession(sc)

PATH_DATA = 'mnt/repo-related/caa-streamworx/caaswx/spark/parquet_data/'
PATH_FLATTENER = 'mnt/repo-related/caa-streamworx/caaswx/spark/parquet_data/flattener/'


class WriteParquet:
    """class to implement various kinds of methods to write parquets"""
    test_dataset = 0
    expected_dataset = 0
    td_file_name = 0
    expected_df_file_name = 0
    td_schema = 0
    expected_data_schema = 0

    def __init__(self, td, ed, td_fn, ed_fn):
        self.test_dataset = td
        self.expected_dataset = ed
        self.td_file_name = td_fn
        self.expected_df_file_name = ed_fn

    # function to write test datasets to parquet files based on a specific
    # schema

    def write_parquet_flattener_user(self):
        """Write datasets into parquet files for testing resources flattener"""
        td_file_path = PATH_FLATTENER + self.td_file_name
        expected_df_file_path = PATH_FLATTENER + self.expected_df_file_name

        # schema for creating a simple test dataset
        test_user_schema = StructType([StructField('SM_USERNAME', StringType()),
                                       StructField(
            'SM_RESOURCE', StringType()),
            StructField('SM_TIMESTAMP_TEMP', StringType())])

        # schema for expected flattener result based on SM_USERNAME
        expected_result_user_schema = StructType([StructField('SM_USERNAME', StringType()),
                                                  StructField('window_temp', StructType([StructField('start', StringType()),
                                                                                         StructField('end',
                                                                                                     StringType())])),
                                                  StructField('SM_RESOURCE', ArrayType(StringType()))])

        test_df = spark.createDataFrame(
            self.test_dataset, schema=test_user_schema)
        test_df = test_df.withColumn('SM_TIMESTAMP',
                                     col('SM_TIMESTAMP_TEMP').cast('timestamp'))
        test_df = test_df.drop('SM_TIMESTAMP_TEMP')
        test_df.write.parquet(td_file_path)

        expected_result_df = spark.createDataFrame(
            self.expected_dataset, schema=expected_result_user_schema)
        expected_result_df = expected_result_df.withColumn('window', col('window_temp').cast(
            StructType([StructField('start', TimestampType()), StructField('end', TimestampType())])))
        expected_result_df = expected_result_df.drop('window_temp')
        expected_result_df = expected_result_df.select(
            'SM_USERNAME', 'window', 'SM_RESOURCE')
        expected_result_df.write.parquet(expected_df_file_path)
        return test_df, expected_result_df

    def write_parquet_general(self, test_schema, expected_result_schema):
        """Write datasets into parquet files by specifying schema"""
        td_file_path = PATH_DATA + self.td_file_name
        expected_df_file_path = PATH_DATA + self.expected_df_file_name
        test_df = spark.createDataFrame(
            self.test_dataset, schema=test_schema)
        test_df.write.parquet(td_file_path)
        expected_result_df = spark.createDataFrame(
            self.expected_dataset, schema=expected_result_schema)
        expected_result_df.write.parquet(expected_df_file_path)
        return test_df, expected_result_df


# obj = WriteParquet(
#     td=,
#     ed=,
#     td_fn='test_data.parquet',
#     ed_fn='expected_data.parquet')
# x, y = obj.write_parquet_general(
#     test_schema=test_user_schema, expected_result_schema=ans_schema)
