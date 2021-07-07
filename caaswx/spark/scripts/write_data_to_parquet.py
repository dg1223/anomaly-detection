from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType
from pyspark.sql.functions import col

sc = SparkContext('local')
spark = SparkSession(sc)

path_flattener = 'mnt/repo-related/caa-streamworx/caaswx/spark/data/flattener/'

# function to write test datasets to parquet files based on a specific schema

def write_parquet_flattener_user(
        test_dataset, expectedDataset, td_file_name, expected_df_file_name):
    td_file_path = path_flattener + td_file_name
    expected_df_file_path = path_flattener + expected_df_file_name

    # schema for creating a simple test dataset
    test_user_schema = StructType([StructField('SM_USERNAME', StringType()),
                                   StructField('SM_RESOURCE', StringType()),
                                   StructField('SM_TIMESTAMP_TEMP', StringType())])

    # schema for expected flattener result based on SM_USERNAME
    expected_result_user_schema = StructType([StructField('SM_USERNAME', StringType()),
                                              StructField('window_temp', StructType([StructField('start', StringType()),
                                                                                     StructField('end',
                                                                                                 StringType())])),
                                              StructField('SM_RESOURCE', ArrayType(StringType()))])

    test_df = spark.createDataFrame(test_dataset, schema=test_user_schema)
    test_df = test_df.withColumn('SM_TIMESTAMP',
                                 col('SM_TIMESTAMP_TEMP').cast('timestamp'))
    test_df = test_df.drop('SM_TIMESTAMP_TEMP')
    test_df.write.parquet(td_file_path)

    expected_result_df = spark.createDataFrame(
        expectedDataset, schema=expected_result_user_schema)
    expected_result_df = expected_result_df.withColumn('window', col('window_temp').cast(
        StructType([StructField('start', TimestampType()), StructField('end', TimestampType())])))
    expected_result_df = expected_result_df.drop('window_temp')
    expected_result_df = expected_result_df.select(
        'SM_USERNAME', 'window', 'SM_RESOURCE')
    expected_result_df.write.parquet(expected_df_file_path)
    return test_df, expected_result_df


