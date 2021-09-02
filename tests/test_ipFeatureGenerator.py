import json
import pathlib

import pyspark.sql.types
from pyspark.sql.session import SparkSession
from src.caaswx.spark._transformers.ipfeaturegenerator import IPFeatureGenerator
from src.caaswx.spark.scripts.nullswap import nullSwap
from src.caaswx.spark.scripts.loadtestdata import load_test_data, load_path

spark = SparkSession.builder.getOrCreate()


def test_1():

    test_df = load_test_data(
        "data", "parquet_data", "ip_feature_generator_tests", "df.parquet"
    )

    ans_1_data = load_test_data(
        "data", "parquet_data", "ip_feature_generator_tests", "ans_data.parquet"
    )

    fg = IPFeatureGenerator()
    result = fg.transform(test_df)

    # content test
    assert result.subtract(ans_1_data).count() == 0

    # row test
    assert result.count() == ans_1_data.count()
