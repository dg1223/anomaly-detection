from src.caaswx.spark._transformers.agentstringflattener import AgentStringFlattener
from src.caaswx.spark.scripts.loadWriteParquet import loadParquet

# if parser is now working, run test_1, edit code in AgentStringFlattener to activate parser and uncomment block below
def test_1():
    data_1 = loadParquet(
        "/home/csl122/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests"
        "/test_data_1_agentflattener.parquet"
    )
    ans_1_data = loadParquet(
        "/home/csl122/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests"
        "/ans_data_1_agentflattener.parquet"
    )
    result = AgentStringFlattener(agentSizeLimit=2, entityName="SM_CLIENTIP", runParser=True
        ).transform(data_1)

    # content test
    assert result.subtract(ans_1_data).count() == 0

    # schema test
    assert result.schema == ans_1_data.schema

    # row test
    assert result.count() == ans_1_data.count()


# if parser is not working, run test_2, edit code in AgentStringFlattener to deactivate parser
def test_2():
    data_2 = loadParquet(
        "/home/csl122/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests"
        "/test_data_2_agentflattener.parquet"
    )
    ans_2_data = loadParquet(
        "/home/csl122/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests"
        "/ans_data_2_agentflattener.parquet"
    )

    result = AgentStringFlattener(agentSizeLimit=2, entityName="SM_CLIENTIP", runParser=True
                                  ).transform(data_2)

    # content test
    assert result.subtract(ans_2_data).count() == 0

    # schema test
    assert result.schema == ans_2_data.schema

    # row test
    assert result.count() == ans_2_data.count()
