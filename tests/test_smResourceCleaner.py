import json

import pyspark.sql.types
from pyspark.sql.session import SparkSession
from src.caaswx.spark.transformers import SMResourceCleaner
from src.caaswx.spark.utils import load_test_data, load_path, null_swap

spark = SparkSession.builder.getOrCreate()


def test_content():
    """
    Tests if the data in the results is the same as the data in the
    expected result.
    """
    df = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "test_data.parquet"
    )

    result = SMResourceCleaner().transform(df)

    ans_1_data = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "ans_data.parquet"
    )

    result_assert = result.subtract(ans_1_data).count()
    ans_assert = ans_1_data.subtract(result).count()

    # content test
    assert (
        result_assert == 0 and ans_assert == 0
    )


def test_schema():
    """
    Tests if the schemas are the same between the result and the expected
    result.
    """
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
    """
    Tests if number of rows are the same between the results and expected
    result.
    """
    df = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "test_data.parquet"
    )

    result = SMResourceCleaner().transform(df)

    ans_1_data = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "ans_data.parquet"
    )

    # row test
    assert result.count() == ans_1_data.count()


def test_empty_data():
    """
    Tests transformer behaviour with an empty dataset.
    """
    df = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "data_empty_df.parquet"
    )

    result = SMResourceCleaner().transform(df)

    assert result.count() == 0
