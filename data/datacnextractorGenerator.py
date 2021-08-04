from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from src.caaswx.spark.scripts.loadWriteParquet import writeParquet


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

    df1 = writeParquet(
        "/home/vmadmin/PycharmProjects/sw-test/sagar/caa-streamworx/data/parquet_data/cn_extractor_tests"
        "/SM_USERNAME_2_examples.parquet",
        testSchema,
        testData,
    )

    df2 = writeParquet(
        "/home/vmadmin/PycharmProjects/sw-test/sagar/caa-streamworx/data/parquet_data/cn_extractor_tests"
        "/expected_SM_USERNAME_2_examples.parquet",
        ansSchema,
        ansData,
    )

    df1.show()
    df2.show()


generateCnExtractorData()
