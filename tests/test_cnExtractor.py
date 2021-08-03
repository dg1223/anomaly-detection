from src.caaswx.spark._transformers.cnextractor import CnExtractor
from src.caaswx.spark.scripts.loadWriteParquet import loadParquet


def test_CnExtractor():
    obj = CnExtractor("SM_USERNAME", "CN")
    testDf = loadParquet(
        "./data\parquet_data/cn_extractor_tests/SM_USERNAME_2_examples" ".parquet"
    )
    answerDf = loadParquet(
        "./data/parquet_data/cn_extractor_tests/expected_SM_USERNAME_2_examples.parquet"
    )
    testDf.show()
    answerDf.show()
    resultDf = obj.transform(testDf)
    sub1 = resultDf.subtract(answerDf)
    sub2 = answerDf.subtract(resultDf)

    assert bool(sub1.head(1)) is bool(sub2.head(1)) is False
