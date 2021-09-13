import json

import pyspark.sql.types
from pyspark.sql.session import SparkSession

from src.caaswx.spark._transformers.userfeaturegenerator import (
    UserFeatureGenerator,
)
from src.caaswx.spark.scripts.loadtestdata import load_test_data, load_path
from src.caaswx.spark.scripts.nullswap import null_swap

spark = SparkSession.builder.getOrCreate()


def test_1():
    test_df = load_test_data(
        "data", "parquet_data", "user_feature_generator_tests", "df.parquet"
    )

    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "user_feature_generator_tests",
        "ans_data.parquet",
    )

    fg = UserFeatureGenerator()
    result = fg.transform(test_df)

    df2_schema_file_path = load_path(
        "data", "JSON", "user_feature_generator_tests", "ans_data_schema.json"
    )

    # ans_1_data = spark.read.json(df2_filePath)
    with open(df2_schema_file_path) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )

    # content test
    assert result.subtract(ans_1_data).count() == 0

    # row test
    assert result.count() == ans_1_data.count()

    # schema test
    null_swap(ans_1_data.schema, ans_1_data_schema)
    assert result.schema == ans_1_data.schema
