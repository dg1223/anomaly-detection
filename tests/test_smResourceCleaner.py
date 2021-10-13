import json

import pyspark.sql.types
from pyspark.sql.session import SparkSession
from src.caaswx.spark._transformers.smresourcecleaner import SMResourceCleaner
from src.caaswx.spark.scripts.nullswap import null_swap
from src.caaswx.spark.scripts.loadtestdata import load_test_data, load_path

spark = SparkSession.builder.getOrCreate()


def test_content():

    df = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "test_data.parquet"
    )

    result = SMResourceCleaner().transform(df)

    ans_1_data = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "ans_data.parquet"
    )

    df2_schema_filepath = load_path(
        "data", "JSON", "sm_resource_tests", "ans_data_schema.json"
    )

    # ans_1_data = spark.read.json(df2_filePath)
    with open(df2_schema_filepath) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )

    # content test
    assert result.subtract(ans_1_data).count() == 0


def test_schema():

    df = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "test_data.parquet"
    )

    result = SMResourceCleaner().transform(df)

    ans_1_data = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "ans_data.parquet"
    )

    df2_schema_filepath = load_path(
        "data", "JSON", "sm_resource_tests", "ans_data_schema.json"
    )

    # ans_1_data = spark.read.json(df2_filePath)
    with open(df2_schema_filepath) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )

    # schema test
    null_swap(ans_1_data.schema, ans_1_data_schema)
    assert result.schema == ans_1_data.schema


def test_num_rows():

    df = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "test_data.parquet"
    )

    result = SMResourceCleaner().transform(df)

    ans_1_data = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "ans_data.parquet"
    )

    df2_schema_filepath = load_path(
        "data", "JSON", "sm_resource_tests", "ans_data_schema.json"
    )

    # ans_1_data = spark.read.json(df2_filePath)
    with open(df2_schema_filepath) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )

    # row test
    assert result.count() == ans_1_data.count()


def test_empty_data():

    df = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "data_empty_df.parquet"
    )

    result = SMResourceCleaner().transform(df)

    assert result.count() == 0