import json
import pathlib

import pyspark.sql.types
from pyspark.sql.session import SparkSession
from src.caaswx.spark._transformers.smresourcecleaner import SMResourceCleaner
from src.caaswx.spark.scripts.nullswap import nullSwap

spark = SparkSession.builder.getOrCreate()


def test_1():
    local_path = pathlib.Path().resolve()
    df = spark.read.parquet(str(local_path)+"/data/parquet_data/sm_resource_tests/test_data.parquet")
    result = SMResourceCleaner().transform(df)

    ans_1_data = spark.read.parquet(str(local_path)+"/data/parquet_data/sm_resource_tests/ans_data.parquet")
    df2_schema_filePath = (
        str(local_path)+"/data/JSON/sm_resource_tests/ans_data_schema.json"
    )

    # ans_1_data = spark.read.json(df2_filePath)
    with open(df2_schema_filePath) as json_file:
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
