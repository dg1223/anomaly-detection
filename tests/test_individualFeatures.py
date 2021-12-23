from datetime import datetime
from pyspark.sql.session import SparkSession
import src.caaswx.spark.features as ft
from src.caaswx.spark.utils import (
    load_test_data,
)
from pytest import raises

spark = SparkSession.builder.getOrCreate()

# Get data
ufg_df = load_test_data(
    "data",
    "parquet_data",
    "user_feature_generator_tests",
    "data.parquet",
)


def test_min_user_timestamp():

    result_df = ft.MinUserTimestamp().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 33, 330000)

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.MinUserTimestamp(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_TIMESTAMP"]).drop(
        "SM_TIMESTAMP"
    )
    result_df = (
        ft.MinUserTimestamp(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 33, 330000)

    result_df = (
        ft.MinUserTimestamp(outputCol="testout")
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
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 33, 330000)


def test_min_time_bt_records():

    result_df = ft.MinTimeBtRecords().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 33, 330000)

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.MinTimeBtRecords(inputCols=["one", "two"]).get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("one", ufg_df["SM_TIMESTAMP"]).drop(
        "SM_TIMESTAMP"
    )
    test_df = test_df.withColumn("two", ufg_df["CN"]).drop("CN")
    result_df = (
        ft.MinTimeBtRecords(inputCols=["one", "two"])
        .get_transformer(["two"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 33, 330000)

    result_df = (
        ft.MinTimeBtRecords(outputCol="testout")
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
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 33, 330000)


def test_max_user_timestamp():

    result_df = ft.MaxUserTimestamp().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 38, 380000)

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.MaxUserTimestamp(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_TIMESTAMP"]).drop(
        "SM_TIMESTAMP"
    )
    result_df = (
        ft.MaxUserTimestamp(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 38, 380000)

    result_df = (
        ft.MaxUserTimestamp(outputCol="testout")
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
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 38, 380000)


def test_max_time_bt_records():

    result_df = ft.MaxTimeBtRecords().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 38, 380000)

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.MaxTimeBtRecords(inputCols=["one", "two"]).get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("one", ufg_df["SM_TIMESTAMP"]).drop(
        "SM_TIMESTAMP"
    )
    test_df = test_df.withColumn("two", ufg_df["CN"]).drop("CN")
    result_df = (
        ft.MaxTimeBtRecords(inputCols=["one", "two"])
        .get_transformer(["two"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 38, 380000)

    result_df = (
        ft.MaxTimeBtRecords(outputCol="testout")
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
    assert result_df.collect()[0][1] == datetime(2018, 2, 1, 0, 0, 38, 380000)


def test_avg_time_bt_records():

    result_df = ft.AvgTimeBtRecords().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 1517443235.79889

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.AvgTimeBtRecords(inputCols=["one", "two"]).get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("one", ufg_df["SM_TIMESTAMP"]).drop(
        "SM_TIMESTAMP"
    )
    test_df = test_df.withColumn("two", ufg_df["CN"]).drop("CN")
    result_df = (
        ft.AvgTimeBtRecords(inputCols=["one", "two"])
        .get_transformer(["two"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 1517443235.79889

    result_df = (
        ft.AvgTimeBtRecords(outputCol="testout")
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
    assert result_df.collect()[0][1] == 1517443235.79889


# Broken, todo later
# def test_user_num_of_accounts_login_with_same_ips():
#
#     result_df = ft.UserNumOfAccountsLoginWithSameIPs()
#     .get_transformer(["CN"]).transform(ufg_df)
#     """
#     Test for default feature functionality
#     """
#     assert result_df.collect()[0][1] == 0
#
#     """
#     Test for valid input column name (if name exists in input dataframe)
#     """
#     with raises(ValueError):
#         ft.UserNumOfAccountsLoginWithSameIPs(inputCol="testin").get_transformer(
#             ["CN"]
#         ).transform(ufg_df)
#
#     test_df = ufg_df.withColumn("testin", ufg_df["SM_USERNAME"]).drop(
#         "SM_USERNAME"
#     )
#     result_df = (
#         ft.UserNumOfAccountsLoginWithSameIPs(inputCol="testin")
#         .get_transformer(["CN"])
#         .transform(test_df)
#     )
#     """
#     Test for input column name change functionality
#     """
#     assert result_df.collect()[0][1] == 0
#
#     result_df = (
#         ft.UserNumOfAccountsLoginWithSameIPs(outputCol="testout")
#         .get_transformer(["CN"])
#         .transform(ufg_df)
#     )
#
#     """
#     Test for output column name change functionality
#     """
#     assert "testout" in result_df.columns
#
#     """
#     Test for correct number of rows in result dataframe with specified output
#     column
#     """
#     assert result_df.count() == 1
#
#     """
#     Test for feature functionality with specified output column
#     """
#     assert result_df.collect()[0][1] == 0
#


def test_std_bt_records():

    result_df = ft.StdBtRecords().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0.88192

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.StdBtRecords(inputCols=["one", "two"]).get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("one", ufg_df["SM_TIMESTAMP"]).drop(
        "SM_TIMESTAMP"
    )
    test_df = test_df.withColumn("two", ufg_df["CN"]).drop("CN")
    result_df = (
        ft.StdBtRecords(inputCols=["one", "two"])
        .get_transformer(["two"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0.88192

    result_df = (
        ft.StdBtRecords(outputCol="testout")
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
    assert result_df.collect()[0][1] == 0.88192


# Broken, todo later
# def test_user_is_using_unusual_browser():
#
#     result_df = ft.UserIsUsingUnusualBrowser()
#       .get_transformer(["CN"]).transform(ufg_df)
#     """
#     Test for default feature functionality
#     """
#     assert result_df.collect()[0][1] == 0.88192
#
#     """
#     Test for valid input column name (if name exists in input dataframe)
#     """
#     with raises(ValueError):
#         ft.UserIsUsingUnusualBrowser(inputCols=["one", "two"])
#         .get_transformer(
#             ["CN"]
#         ).transform(ufg_df)
#
#     test_df = ufg_df.withColumn("one", ufg_df["SM_AGENTNAME"]).drop(
#         "SM_AGENTNAME"
#     )
#     test_df = test_df.withColumn("two", ufg_df["CN"]).drop(
#         "CN"
#     )
#     result_df = (
#         ft.UserIsUsingUnusualBrowser(inputCols=["one", "two"])
#         .get_transformer(["two"])
#         .transform(test_df)
#     )
#     """
#     Test for input column name change functionality
#     """
#     assert result_df.collect()[0][1] == 0.88192
#
#     result_df = (
#         ft.UserIsUsingUnusualBrowser(outputCol="testout")
#         .get_transformer(["CN"])
#         .transform(ufg_df)
#     )
#
#     """
#     Test for output column name change functionality
#     """
#     assert "testout" in result_df.columns
#
#     """
#     Test for correct number of rows in result dataframe with specified output
#     column
#     """
#     assert result_df.count() == 1
#
#     """
#     Test for feature functionality with specified output column
#     """
#     assert result_df.collect()[0][1] == 0.88192
#


def test_flattener_feature():

    result_df = (
        ft.FlattenerFeature("SM_AGENTNAME", "Flattened_SM_AGENTNAME")
        .get_transformer(["CN"])
        .transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == [
        "httpd-cra-ssl-cmscpext-p103-07-03",
        "httpd-cra-ssl-cmsrpext-p102-07-03",
        "httpd-cra-ssl-cmsrpext-p102-01-03",
    ]

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.FlattenerFeature(
            "wroung_in", "Flattened_SM_AGENTNAME"
        ).get_transformer(["CN"]).transform(ufg_df)

    """
    Test for output column name change functionality
    """
    assert "Flattened_SM_AGENTNAME" in result_df.columns

    """
    Test for correct number of rows in result dataframe with specified output
    column
    """
    assert result_df.count() == 1
