from src.caaswx.spark._transformers.agentstringflattener import AgentStringFlattener
from src.caaswx.spark.scripts.loadtestdata import load_test_data
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

spark = SparkSession.builder.getOrCreate()


def test_1():
    df = spark.read.parquet("./data/parquet_data/agentStringFlattener_tests/df.parquet")
    result = AgentStringFlattener(
        agentSizeLimit=2, entityName="SM_CLIENTIP", runParser=True
    ).transform(df)

    ans_1_data = spark.read.parquet(
        "./data/parquet_data/agentStringFlattener_tests/ans_1_df.parquet"
    )

    df2_schema_filePath = (
        "./data/JSON/agent_flattener_tests/ans_data_1_schema_agentflattener.json"
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


def test_2():
    df = spark.read.parquet("./data/parquet_data/agentStringFlattener_tests/df.parquet")
    result = AgentStringFlattener(
        agentSizeLimit=2, entityName="SM_CLIENTIP", runParser=False
    ).transform(df)

    ans_2_data = spark.read.parquet(
        "./data/parquet_data/agentStringFlattener_tests/ans_2_df.parquet"
    )

    df4_schema_filePath = (
        "./data/JSON/agent_flattener_tests/ans_data_2_schema_agentflattener.json"
    )
    with open(df4_schema_filePath) as json_file:
        ans_2_data_schema = json.load(json_file)

    ans_2_data_schema = pyspark.sql.types.StructType.fromJson(
        json.loads(ans_2_data_schema)
    )

    # content test
    assert result.subtract(ans_2_data).count() == 0

    # schema test
    nullSwap(ans_2_data.schema, ans_2_data_schema)
    print(result.schema)
    print(ans_2_data_schema)
    print(ans_2_data.schema)
    assert result.schema == ans_2_data.schema

    # row test
    assert result.count() == ans_2_data.count()
