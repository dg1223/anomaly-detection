from pyspark.sql.session import SparkSession
from pytest import raises

import src.caaswx.spark.features as ft
from src.caaswx.spark.utils import load_test_data

spark = SparkSession.builder.getOrCreate()

# Get data
ufg_df = load_test_data(
    "data",
    "parquet_data",
    "user_feature_generator_tests",
    "data.parquet",
)


def test_count_unique_ou():

    result_df = ft.CountUniqueOU().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountUniqueOU(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_USERNAME"]).drop(
        "SM_USERNAME"
    )
    result_df = (
        ft.CountUniqueOU(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountUniqueOU(outputCol="testout")
        .get_transformer(["CN"])
        .transform(ufg_df)
    )

    """
    Test for output column name change functionality
    """
    assert "testout" in result_df.columns

    """
    Test for correct number of rows in result dataframe with specified output 
    column
    """
    assert result_df.count() == 1

    """
    Test for feature functionality with specified output column
    """
    assert result_df.collect()[0][1] == 0


def test_count_unique_rep():
    group_keys = ["CN"]
    result = ft.CountUniqueRep().get_transformer(group_keys).transform(ufg_df)
    """
    Test for default functionality
    """
    assert result.collect()[0][1] == 0

    """
    Test for name update
    """
    with raises(ValueError):
        (
            ft.UniquePortalRep(inputCol="testInput", outputCol="testOutput")
            .get_transformer(group_keys)
            .transform(ufg_df)
        )

    test = ufg_df.withColumn("testInput", ufg_df["SM_RESOURCE"]).drop("SM_RESOURCE")

    result = (
        ft.CountUniqueRep(inputCol="testInput", outputCol="testOutput")
        .get_transformer(group_keys)
        .transform(test)
    )

    assert "CN" in result.columns

    assert "testOutput" in result.columns

    """
    Test for default functionality
    """
    assert result.collect()[0][1] == 0

    """
    Correct number of result rows
    """
    assert result.count() == 1


def test_count_unique_user_app():
    group_keys = ["CN"]
    result = ft.CountUniqueOU().get_transformer(group_keys).transform(ufg_df)
    """
    Default test to check basic functionality.
    """
    assert result.collect()[0][1] == 0

    """
    Check for name exist
    """
    with raises(ValueError):
        (
            ft.CountUniqueUserApps(inputCol="testInput", outputCol="testOutput")
            .get_transformer(group_keys)
            .transform(ufg_df)
        )

    """
    Test name change.
    """
    test = ufg_df.withColumn("testInput", ufg_df["CN"]).drop("CN")
    result = (
        ft.CountUniqueUserApps(inputCol="testInput", outputCol="testOutput")
        .get_transformer(["testInput"])
        .transform(test)
    )

    """
    Test for input and output col name change functionality.
    """
    assert "testInput" in result.columns

    assert "testOutput" in result.columns

    """
    Correct number of result rows
    """
    assert result.count() == 1
