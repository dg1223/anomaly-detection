from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from src.caaswx.spark.scripts import loadWriteParquet

sc = SparkContext("local")
spark = SparkSession(sc)


def generateCnExtractorData():
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

    df1 = loadWriteParquet.writeParquet("F:\CRA\caa-streamworx\data\parquet_data\cn_extractor_tests\\",
                                        "SM_USERNAME_2_example.parquet",
                                        testSchema, testData)

    df2 = loadWriteParquet.writeParquet("F:\CRA\caa-streamworx\data\parquet_data\cn_extractor_tests\\",
                                        "expected_SM_USERNAME_2_example.parquet", ansSchema, ansData)

    df1.show()
    df2.show()


generateCnExtractorData()
