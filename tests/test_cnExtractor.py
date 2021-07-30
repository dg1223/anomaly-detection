from data.datacnextractor import DataCnExtractor
from src.caaswx.spark._transformers.cnextractor import CnExtractor


def test_CnExtractor():
    obj = CnExtractor("SM_USERNAME", "CN")
    testDf, answerDf = DataCnExtractor().generateCnExtractorData()
    testDf.show()
    answerDf.show()
    resultDf = obj.transform(testDf)
    resultDf.show()
    # sub1 = resultDf.subtract(answerDf)
    # sub2 = answerDf.subtract(resultDf)

    # assert bool(sub1.head(1)) is bool(sub2.head(1)) is False
