from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext

conf = SparkConf()
conf.setAppName("caaswx")
sc = SparkContext(conf=conf)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

spark = SQLContext(sc)


class DataCnExtractor:
    def generateCnExtractorData(self):
        # #     testSchema = StructType([StructField("SM_USERNAME", StringType())])
        # #     testData = ["cn=hasgdvfashdgfahg,ou=Credential,ou=PR", "asjkdhfbjksdlbaf"]
        # #     ansSchema = StructType(
        # #         [StructField("SM_USERNAME", StringType()), StructField("CN", StringType())]
        # #     )
        # #     answerData = [
        # #         ("cn=hasgdvfashdgfahg,ou=Credential,ou=PR", "hasgdvfashdgfahg"),
        # #         ("asjkdhfbjksdlbaf", "asjkdhfbjksdlbaf"),
        # #     ]
        # #     testDf = spark.createDataFrame(testData, schema=testSchema)
        # #     answerDf = spark.createDataFrame(answerData, schema=ansSchema)
        #
        #     return testDf, answerDf

        testSchema = StructType(
            [
                StructField("SM_USERNAME", StringType()),
            ]
        )

        testData = [
            ("cn=hasgdvfashdgfahg,ou=Credential,ou=PR",),
            ("asjkdhfbjksdlbaf",),
        ]

        ansSchema = StructType(
            [
                StructField("SM_USERNAME", StringType()),
                StructField("CN", StringType()),
            ]
        )

        ansData = [
            ("cn=hasgdvfashdgfahg,ou=Credential,ou=PR", "hasgdvfashdgfahg"),
            ("asjkdhfbjksdlbaf", "asjkdhfbjksdlbaf"),
        ]
        df = spark.createDataFrame(testData, testSchema)

        df2 = spark.createDataFrame(ansData, ansSchema)

        return df, df2
