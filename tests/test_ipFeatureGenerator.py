import json

import pyspark.sql.types
from pyspark.sql.session import SparkSession
from src.caaswx.spark._transformers.ipfeaturegenerator import (
    IPFeatureGenerator,
)
from src.caaswx.spark.utilities.loadtestdata import load_test_data, load_path
from src.caaswx.spark.utilities.schema_utils import null_swap

spark = SparkSession.builder.getOrCreate()


def test_content():
    """
    Tests if the data in the results is the same as the data in the
    expected result.
    """
    test_df = load_test_data(
        "data", "parquet_data", "ip_feature_generator_tests", "data_df.parquet"
    )

    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "ip_feature_generator_tests",
        "ans_data" ".parquet",
    )

    fg = IPFeatureGenerator()
    result = fg.transform(test_df)

    # content test
    assert result.subtract(ans_1_data).count() == 0


def test_num_rows():
    """
    Tests if number of rows are the same between the results and expected
    result.
    """
    test_df = load_test_data(
        "data", "parquet_data", "ip_feature_generator_tests", "data_df.parquet"
    )

    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "ip_feature_generator_tests",
        "ans_data" ".parquet",
    )
    fg = IPFeatureGenerator()
    result = fg.transform(test_df)

    # row test
    assert result.count() == ans_1_data.count()


def test_schema():
    """
    Tests if the schemas are the same between the result and the expected
    result.
    """
    test_df = load_test_data(
        "data", "parquet_data", "ip_feature_generator_tests", "data_df.parquet"
    )

    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "ip_feature_generator_tests",
        "ans_data" ".parquet",
    )

    df2_schema_file_path = load_path(
        "data", "JSON", "ip_feature_generator_tests", "ans_data_schema.json"
    )

    with open(df2_schema_file_path) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )
    fg = IPFeatureGenerator()
    result = fg.transform(test_df)

    # schema test
    null_swap(ans_1_data.schema, ans_1_data_schema)
    assert result.schema == ans_1_data.schema


def test_empty_data():
    """
    Tests transformer behaviour with an empty dataset.
    """
    test_df = load_test_data(
        "data",
        "parquet_data",
        "ip_feature_generator_tests",
        "data_empty_df.parquet",
    )

    fg = IPFeatureGenerator()
    result = fg.transform(test_df)

    assert result.count() == 0
