import json
import pathlib

import pyspark.sql.types
from pyspark.sql.session import SparkSession
from src.caaswx.spark._transformers.sessionfeaturegenerator import (
    SessionFeatureGenerator,
)
from src.caaswx.spark.scripts.nullswap import nullSwap
from src.caaswx.spark.scripts.loadtestdata import load_test_data, load_path

spark = SparkSession.builder.getOrCreate()


def test_1():

    test_df = spark.read.parquet(
        "data/parquet_data/session_feature_generator_tests/df.parquet"
    )
    ans_1_data = spark.read.parquet(
        "./data/parquet_data/session_feature_generator_tests/ans_data.parquet"
    )

    fg = SessionFeatureGenerator()
    result = fg.transform(test_df)

    # content test
    assert result.subtract(ans_1_data).count() == 0

    # row test
    assert result.count() == ans_1_data.count()
