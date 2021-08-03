from pyspark.shell import spark
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


class DataCnExtractor:
    def generateCnExtractorData(self):
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
