from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)
from pytest import raises
from src.caaswx.spark.utils import schema_test


def test_different_schemas():
    """
    Test Column Name Difference.
    """
    schema = StructType([StructField("EVENTID", IntegerType())])
    schema1 = StructType([StructField("SomeCol", IntegerType())])
    with raises(ValueError):
        schema_test(schema1, schema)


def test_same_schemas():
    """
    Test two similar schemas.
    """
    schema = StructType([StructField("EVENTID", IntegerType())])
    schema1 = StructType([StructField("EVENTID", IntegerType())])

    assert schema_test(schema1, schema) is None


def test_subset_schema():
    """
    Test schema is a subset.
    """
    schema_superset = StructType(
        [
            StructField("COL1", IntegerType()),
            StructField("COL2", DoubleType()),
            StructField("COL3", DoubleType()),
            StructField("COL4", StringType()),
        ]
    )
    schema_subset = StructType(
        [
            StructField("COL1", IntegerType()),
            StructField("COL2", DoubleType()),
        ]
    )

    assert (
        schema_test(test_schema=schema_subset, base_schema=schema_superset)
        is None
    )


def test_windowed_schema():
    """
    Testing Layered Schemas.
    """
    layered_1_super = StructType(
        [
            StructField(
                "window",
                StructType(
                    [
                        StructField("start", StringType()),
                        StructField("end", StringType()),
                    ]
                ),
                False,
            ),
            StructField("COL1", IntegerType()),
            StructField("COL2", DoubleType()),
        ]
    )

    layered_1 = StructType(
        [
            StructField(
                "window",
                StructType(
                    [
                        StructField("start", StringType()),
                        StructField("end", StringType()),
                    ]
                ),
                False,
            ),
        ]
    )
    layered_2 = StructType(
        [
            StructField(
                "window",
                StructType(
                    [
                        StructField("someDifference", StringType()),
                        StructField("end", StringType()),
                    ]
                ),
                False,
            ),
        ]
    )
    layered_3 = StructType(
        [
            StructField(
                "window",
                StructType(
                    [
                        StructField("start", IntegerType()),
                        StructField("end", StringType()),
                    ]
                ),
                False,
            ),
        ]
    )
    # Layered Subset test.
    assert (
        schema_test(test_schema=layered_1, base_schema=layered_1_super) is None
    )

    with raises(ValueError):
        schema_test(layered_1, layered_2)  # name difference one layer down
        schema_test(layered_1, layered_3)  # datatype difference one layer down
