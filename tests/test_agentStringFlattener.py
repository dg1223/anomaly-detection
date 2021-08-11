from src.caaswx.spark._transformers.agentstringflattener import AgentStringFlattener
from src.caaswx.spark.scripts.loadWriteParquet import loadParquet
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.getOrCreate()
import json

# if parser is now working, run test_1, edit code in AgentStringFlattener to activate parser and uncomment block below
def test_1():
    df1_filePath = "/home/nds838/Documents/caa-streamworx/data/JSON/agent_flattener_tests/test_data_1_agentflattener.json"

    # data_1 = spark.read.json(df1_filePath)

    # df2_filePath = "/home/nds838/Documents/caa-streamworx/data/JSON/agent_flattener_tests/ans_data_1_agentflattener.json"
    df2_schema_filePath = "/home/nds838/Documents/caa-streamworx/data/JSON/agent_flattener_tests/ans_data_1_schema_agentflattener.json"

    # ans_1_data = spark.read.json(df2_filePath)
    with open(df2_schema_filePath) as json_file:
        ans_1_data_schema = json.load(json_file)
    data_1 = loadParquet(
        "./data/parquet_data/agent_flattener_tests"
        "/test_data_1_agentflattener.parquet"
    )
    ans_1_data = loadParquet(
        "./data/parquet_data/agent_flattener_tests"
        "/ans_data_1_agentflattener.parquet"
    )
    result = AgentStringFlattener(agentSizeLimit=2, entityName="SM_CLIENTIP", runParser=True
        ).transform(data_1)
    result.show()
    print(result.schema)
    ans_1_data.show()
    print(ans_1_data_schema)
    # content test
    assert result.subtract(ans_1_data).count() == 0

    # schema test
    assert result.schema == ans_1_data_schema

    # row test
    assert result.count() == ans_1_data.count()

# if parser is not working, run test_2, edit code in AgentStringFlattener to deactivate parser
def test_2():
    data_2 = loadParquet(
        "./data/parquet_data/agent_flattener_tests"
        "/test_data_2_agentflattener.parquet"
    )
    ans_2_data = loadParquet(
        "./data/parquet_data/agent_flattener_tests"
        "/ans_data_2_agentflattener.parquet"
    )
    df4_schema_filePath = "/home/nds838/Documents/caa-streamworx/data/JSON/agent_flattener_tests/ans_data_2_schema_agentflattener.json"
    with open(df4_schema_filePath) as json_file:
        ans_1_data_schema = json.load(json_file)
    result = AgentStringFlattener(agentSizeLimit=2, entityName="SM_CLIENTIP", runParser=False
                                  ).transform(data_2)

    result.show()
    print(result.schema)
    ans_2_data.show()
    print(ans_2_data.schema)
    # content test
    assert result.subtract(ans_2_data).count() == 0

    # schema test
    assert str(result.schema) == ans_1_data_schema

    # row test
    assert result.count() == ans_2_data.count()

# test_1()