from pyspark.sql.session import SparkSession
import src.caaswx.spark.transformers as transform

spark = SparkSession.builder.getOrCreate()

"""
This set of tests are intended to test the intercompatibility of the separate 
parts of the pipeline.
 
test_CN_RC_UserFG tests:
 CnExtractor -> SMResourceCleaner -> UserFeatureGenerator
test ASF_RF_SessionFG will test:
 AgentStringFlattner -> ResourceFlattener -> ServerFeatureGenertor
    ASF and RF currently return new dataframes with just the respective new 
    columns, therefore will not work.
"""


def test_CN_RC_UserFG():
    """
    Tests if the CnExtractor, SMResouceCleaner and UserFeatureGenertors
    work together with a sample of raw_logs.
    """

    df = spark.read.parquet("./data/parquet_data/pipeline_tests/data.parquet")
    resultCnExtractor = transform.CnExtractor().transform(df)
    resultSMResourceCleaner = transform.SMResourceCleaner().transform(
        resultCnExtractor
    )
    resultFG = transform.UserFeatureGenerator().transform(
        resultSMResourceCleaner
    )

    assert resultFG.count() == 1
