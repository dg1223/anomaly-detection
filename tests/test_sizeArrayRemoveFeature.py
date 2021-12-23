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
        ft.CountUniqueOU(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

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
    Test for default feature functionality
    """
    assert result.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        (
            ft.CountUniqueRep(inputCol="testInput", outputCol="testOutput")
            .get_transformer(group_keys)
            .transform(ufg_df)
        )

    test = ufg_df.withColumn("testInput", ufg_df["SM_RESOURCE"]).drop(
        "SM_RESOURCE"
    )

    result = (
        ft.CountUniqueRep(inputCol="testInput", outputCol="testOutput")
        .get_transformer(group_keys)
        .transform(test)
    )

    """
    Test for output column name change functionality
    """
    assert "testOutput" in result.columns

    """
    Test for correct number of rows in result dataframe with specified output
    column
    """
    assert result.count() == 1

    """
    Test for feature functionality with specified output column
    """
    assert result.collect()[0][1] == 0


def test_count_unique_user_apps():
    group_keys = ["CN"]
    result = (ft.CountUniqueUserApps()
              .get_transformer(group_keys)
              .transform(ufg_df)
              )
    """
    Test for default feature functionality
    """
    result.show()
    assert result.collect()[0][1] == 3

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        (
            ft.CountUniqueUserApps(
                inputCol="testInput", outputCol="testOutput"
            )
            .get_transformer(group_keys)
            .transform(ufg_df)
        )

    """
    Test for input column name change functionality
    """
    test = ufg_df.withColumn("testInput", ufg_df["SM_RESOURCE"])\
        .drop("SM_RESOURCE")
    result = (
        ft.CountUniqueUserApps(inputCol="testInput", outputCol="testOutput")
        .get_transformer(group_keys)
        .transform(test)
    )

    """
    Test for output column name change functionality
    """
    assert "testOutput" in result.columns

    """
    Test for correct number of rows in result dataframe with specified output
    column
    """
    assert result.count() == 1

    """
    Test for feature functionality with specified output column
    """
    assert result.collect()[0][1] == 3
