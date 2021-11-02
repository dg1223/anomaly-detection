from src.caaswx.spark.utilities.nullswap import null_swap
from pyspark.sql.session import SparkSession
import json
import pyspark.sql.types
from src.caaswx.spark._transformers.agentstringflattener import (
    AgentStringFlattener,
)

spark = SparkSession.builder.getOrCreate()


def test_content():
    """
    Tests if the data in the results is the same as the data in the
    expected result.
    """
    df = spark.read.parquet(
        "./data/parquet_data/agentStringFlattener_tests/data_df.parquet"
    )
    result = AgentStringFlattener(
        agent_size_limit=2, agg_col="SM_CLIENTIP"
    ).transform(df)

    ans_1_data = spark.read.parquet(
        "./data/parquet_data/agentStringFlattener_tests/ans_1_df.parquet"
    )

    # content test
    assert result.subtract(ans_1_data).count() == 0


def test_schema():
    """
    Tests if the schemas are the same between the result and the expected
    result.
    """
    df = spark.read.parquet(
        "./data/parquet_data/agentStringFlattener_tests/data_df.parquet"
    )
    result = AgentStringFlattener(
        agent_size_limit=2, agg_col="SM_CLIENTIP"
    ).transform(df)

    ans_1_data = spark.read.parquet(
        "./data/parquet_data/agentStringFlattener_tests/ans_1_df.parquet"
    )

    df2_schema_file_path = (
        "./data/JSON/agent_flattener_tests/ans_data_1_schema_agentflattener"
        ".json"
    )

    with open(df2_schema_file_path) as json_file:
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
    df = spark.read.parquet(
        "./data/parquet_data/agentStringFlattener_tests/data_df.parquet"
    )
    result = AgentStringFlattener(
        agent_size_limit=2, agg_col="SM_CLIENTIP"
    ).transform(df)

    ans_1_data = spark.read.parquet(
        "./data/parquet_data/agentStringFlattener_tests/ans_1_df.parquet"
    )

    # row test
    assert result.count() == ans_1_data.count()


def test_empty_data():
    """
    Tests transformer behaviour with an empty dataset.
    """
    df = spark.read.parquet(
        "./data/parquet_data/agentStringFlattener_tests/data_empty_df.parquet"
    )

    result = AgentStringFlattener(
        agent_size_limit=2, agg_col="SM_CLIENTIP"
    ).transform(df)

    assert result.count() == 0
