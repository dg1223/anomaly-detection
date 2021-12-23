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


def test_count_unique_actions():

    result_df = (
        ft.CountUniqueActions().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 1

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountUniqueActions(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_ACTION"]).drop(
        "SM_ACTION"
    )
    result_df = (
        ft.CountUniqueActions(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 1

    result_df = (
        ft.CountUniqueActions(outputCol="testout")
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
    assert result_df.collect()[0][1] == 1


def test_count_unique_events():

    result_df = (
        ft.CountUniqueEvents().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 1

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountUniqueEvents(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountUniqueEvents(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 1

    result_df = (
        ft.CountUniqueEvents(outputCol="testout")
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
    assert result_df.collect()[0][1] == 1


def test_count_unique_sessions():

    result_df = (
        ft.CountUniqueSessions().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 3

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountUniqueSessions(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_SESSIONID"]).drop(
        "SM_SESSIONID"
    )
    result_df = (
        ft.CountUniqueSessions(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 3

    result_df = (
        ft.CountUniqueSessions(outputCol="testout")
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
    assert result_df.collect()[0][1] == 3


def test_count_unique_username():

    result_df = (
        ft.CountUniqueUsername().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 1

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountUniqueUsername(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["CN"]).drop("CN")
    result_df = (
        ft.CountUniqueUsername(inputCol="testin")
        .get_transformer(["testin"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 1

    result_df = (
        ft.CountUniqueUsername(outputCol="testout")
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
    assert result_df.collect()[0][1] == 1


def test_count_unique_resources():

    result_df = (
        ft.CountUniqueResources().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 6

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountUniqueResources(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_RESOURCE"]).drop(
        "SM_RESOURCE"
    )
    result_df = (
        ft.CountUniqueResources(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 6

    result_df = (
        ft.CountUniqueResources(outputCol="testout")
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
    assert result_df.collect()[0][1] == 6


def test_count_unique_ips():

    result_df = ft.CountUniqueIps().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 2

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountUniqueIps(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["SM_CLIENTIP"]).drop(
        "SM_CLIENTIP"
    )
    result_df = (
        ft.CountUniqueIps(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 2

    result_df = (
        ft.CountUniqueIps(outputCol="testout")
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
    assert result_df.collect()[0][1] == 2
