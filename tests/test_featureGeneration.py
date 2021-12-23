from pyspark.sql.session import SparkSession
import src.caaswx.spark.transformers as transform

spark = SparkSession.builder.getOrCreate()

"""
This set of tests are intended to test the inter-compatibility of the separate
parts of the pipeline.

test_CN_RC_UserFG tests:
 CnExtractor -> SMResourceCleaner -> UserFeatureGenerator

test ASF_RF_SessionFG will test:
 AgentStringFlattener -> ResourceFlattener -> ServerFeatureGenerator
    ASF and RF currently return new dataframes with just the respective new
    columns, therefore will not work.
"""


def test_cn_rc_userfg():
    """
    Tests if the CnExtractor, SMResourceCleaner and UserFeatureGenerators
    work together with a sample of raw_logs.
    """

    df = spark.read.parquet("./data/parquet_data/pipeline_tests/data.parquet")
    result_cnextractor = transform.CnExtractor().transform(df)
    result_smresourcecleaner = transform.SMResourceCleaner().transform(
        result_cnextractor
    )
    result_fg = transform.UserFeatureGenerator().transform(
        result_smresourcecleaner
    )

    assert result_fg.count() == 1
