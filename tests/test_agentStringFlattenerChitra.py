from src.caaswx.spark._transformers.agentstringflattener import AgentStringFlattener
from src.caaswx.spark.scripts.loadtestdata import load_test_data
from src.caaswx.spark.scripts.nullswap import nullSwap
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()

def test_1():
    df = spark.read.parquet('/home/vmadmin/PycharmProjects/sw-test/Usman/caa-streamworx/data/parquet_data/agentStringFlattener_tests/df.parquet')
    result = AgentStringFlattener(agentSizeLimit=2, entityName="SM_CLIENTIP", runParser=True).transform(df)

    ans_1_data = spark.read.parquet('/home/vmadmin/PycharmProjects/sw-test/Usman/caa-streamworx/data/parquet_data/agentStringFlattener_tests/ans_1_df.parquet')

    # content test
    assert (result.subtract(ans_1_data).count() == 0)

    # schema test
    nullSwap(result.schema, ans_1_data.schema)
    assert(result.schema == ans_1_data.schema)

    # row test
    assert (result.count() == ans_1_data.count())

def test_2():
    df = spark.read.parquet(
        '/home/vmadmin/PycharmProjects/sw-test/Usman/caa-streamworx/data/parquet_data/agentStringFlattener_tests/df.parquet')
    result = AgentStringFlattener(agentSizeLimit=2, entityName="SM_CLIENTIP", runParser=False).transform(df)

    ans_2_data = spark.read.parquet(
        '/home/vmadmin/PycharmProjects/sw-test/Usman/caa-streamworx/data/parquet_data/agentStringFlattener_tests/ans_2_df.parquet')

    # content test
    assert (result.subtract(ans_2_data).count() == 0)

    # schema test
    nullSwap(result.schema, ans_2_data.schema)
    assert (result.schema == ans_2_data.schema)

    # row test
    assert (result.count() == ans_2_data.count())
