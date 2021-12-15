import json
import pyspark.sql.types
from pyspark.sql.session import SparkSession
from src.caaswx.spark.transformers import (
    ServerFeatureGenerator,
)
from src.caaswx.spark.utils import null_swap, load_test_data, load_path

spark = SparkSession.builder.getOrCreate()


def test_no_of_users():
    fg = ServerFeatureGenerator()
    test_df = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "user2_one_window_df.parquet",
    )

    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "expected_2user_one_window_df.parquet",
    )

    result = fg.transform(test_df)

    df2_schema_file_path = load_path(
        "data",
        "JSON",
        "server_feature_generator_tests",
        "ans_data_schema" ".json ",
    )

    with open(df2_schema_file_path) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )

    result_assert = result.subtract(ans_1_data).count()
    ans_assert = ans_1_data.subtract(result).count()

    # content test
    assert result_assert == 0 and ans_assert == 0

    # row test
    assert result.count() == ans_1_data.count()

    # schema test

    null_swap(ans_1_data.schema, ans_1_data_schema)
    assert result.schema == ans_1_data.schema


def test_failed_logins():
    fg = ServerFeatureGenerator()
    test_df = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "user5_one_window_df.parquet",
    )
    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "expected_5user_one_window_df.parquet",
    )

    result = fg.transform(test_df)

    df2_schema_file_path = load_path(
        "data",
        "JSON",
        "server_feature_generator_tests",
        "ans_data_schema.json",
    )

    with open(df2_schema_file_path) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )

    result_assert = result.subtract(ans_1_data).count()
    ans_assert = ans_1_data.subtract(result).count()

    # content test
    assert result_assert == 0 and ans_assert == 0

    # row test
    assert result.count() == ans_1_data.count()

    # schema test

    null_swap(ans_1_data.schema, ans_1_data_schema)
    assert result.schema == ans_1_data.schema


def test_mulitple_ip_fails():
    fg = ServerFeatureGenerator()
    test_df = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "user1_ip2_one_window_df.parquet",
    )
    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "expected_1user_ip2_one_window_df.parquet",
    )

    result = fg.transform(test_df)

    df2_schema_file_path = load_path(
        "data",
        "JSON",
        "server_feature_generator_tests",
        "ans_data_schema.json",
    )

    with open(df2_schema_file_path) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )

    result_assert = result.subtract(ans_1_data).count()
    ans_assert = ans_1_data.subtract(result).count()

    # content test
    assert result_assert == 0 and ans_assert == 0

    # row test
    assert result.count() == ans_1_data.count()

    # schema test

    null_swap(ans_1_data.schema, ans_1_data_schema)
    assert result.schema == ans_1_data.schema


def test_two_windows():
    fg = ServerFeatureGenerator()
    test_df = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "user1_ip1_two_window_df.parquet",
    )
    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "expected_1user_ip1_two_window_df.parquet",
    )

    result = fg.transform(test_df)

    df2_schema_file_path = load_path(
        "data",
        "JSON",
        "server_feature_generator_tests",
        "ans_data_schema.json",
    )

    with open(df2_schema_file_path) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )

    result_assert = result.subtract(ans_1_data).count()
    ans_assert = ans_1_data.subtract(result).count()

    # content test
    assert result_assert == 0 and ans_assert == 0

    # row test
    assert result.count() == ans_1_data.count()

    # schema test

    null_swap(ans_1_data.schema, ans_1_data_schema)
    assert result.schema == ans_1_data.schema


def test_two_windows_multiple_logins():
    fg = ServerFeatureGenerator()
    test_df = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "user1_two_window_multiple_events_df.parquet",
    )
    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "expected_1user_two_window_multiple_events_df.parquet",
    )

    result = fg.transform(test_df)

    df2_schema_file_path = load_path(
        "data",
        "JSON",
        "server_feature_generator_tests",
        "ans_data_schema.json",
    )

    with open(df2_schema_file_path) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )

    result_assert = result.subtract(ans_1_data).count()
    ans_assert = ans_1_data.subtract(result).count()

    # content test
    assert result_assert == 0 and ans_assert == 0

    # row test
    assert result.count() == ans_1_data.count()

    # schema test

    null_swap(ans_1_data.schema, ans_1_data_schema)
    assert result.schema == ans_1_data.schema


def test_two_windows_multiple_ips():
    fg = ServerFeatureGenerator()
    test_df = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "user2_two_window_multiple_ips_df.parquet",
    )
    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "expected_2user_two_window_multiple_ips_df.parquet",
    )

    result = fg.transform(test_df)

    df2_schema_file_path = load_path(
        "data",
        "JSON",
        "server_feature_generator_tests",
        "ans_data_schema.json",
    )

    with open(df2_schema_file_path) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )

    result_assert = result.subtract(ans_1_data).count()
    ans_assert = ans_1_data.subtract(result).count()

    # content test
    assert result_assert == 0 and ans_assert == 0

    # row test
    assert result.count() == ans_1_data.count()

    # schema test

    null_swap(ans_1_data.schema, ans_1_data_schema)
    assert result.schema == ans_1_data.schema


def test_empty_data():
    fg = ServerFeatureGenerator()
    test_df = load_test_data(
        "data",
        "parquet_data",
        "server_feature_generator_tests",
        "data_empty_df.parquet",
    )

    result = fg.transform(test_df)

    assert result.count() == 0
