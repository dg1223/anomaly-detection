from src.caaswx.spark._transformers.smresourcecleaner import SMResourceCleaner
from src.caaswx.spark.scripts.loadtestdata import load_test_data
from src.caaswx.spark.scripts.loadtestdata import load_path
from src.caaswx.spark.scripts.nullswap import nullSwap
from pyspark.sql.types import StructType
from pyspark.sql.session import SparkSession
import json
import json
import pyspark.sql.functions as f
import pyspark.sql.types
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    TimestampType,
)
from pyspark.sql.session import SparkSession
import os
import pathlib

spark = SparkSession.builder.getOrCreate()


def test_1():
    df = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "test_data.parquet"
    )
    json_schema_path = load_path("data", "JSON", "sm_resource_tests", "ans_data_schema.json")
    result = SMResourceCleaner().transform(df)

    ans_1_data = load_test_data(
        "data", "parquet_data", "sm_resource_tests", "ans_data.parquet"
    )

    with open(json_schema_path) as json_file:
        ans_1_data_schema = json.load(json_file)

    ans_1_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_1_data_schema)
    )

    # content test
    assert result.subtract(ans_1_data).count() == 0

    # schema test
    nullSwap(ans_1_data.schema, ans_1_data_schema)
    assert result.schema == ans_1_data.schema

    # row test
    assert result.count() == ans_1_data.count()
