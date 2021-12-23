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


def test_count_auth_accept():

    result_df = ft.CountAuthAccept().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountAuthAccept(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountAuthAccept(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountAuthAccept(outputCol="testout")
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


def test_count_auth_reject():

    result_df = ft.CountAuthReject().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountAuthReject(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountAuthReject(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountAuthReject(outputCol="testout")
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


def test_count_admin_attempt():

    result_df = (
        ft.CountAdminAttempt().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountAdminAttempt(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountAdminAttempt(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountAdminAttempt(outputCol="testout")
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


def test_count_auth_challenge():

    result_df = (
        ft.CountAuthChallenge().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountAuthChallenge(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountAuthChallenge(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountAuthChallenge(outputCol="testout")
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


def test_count_az_accept():

    result_df = ft.CountAZAccept().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountAZAccept(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountAZAccept(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountAZAccept(outputCol="testout")
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


def test_count_az_reject():

    result_df = ft.CountAZReject().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountAZReject(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountAZReject(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountAZReject(outputCol="testout")
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


def test_count_admin_login():

    result_df = ft.CountAdminLogin().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountAdminLogin(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountAdminLogin(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountAdminLogin(outputCol="testout")
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


def test_count_admin_logout():

    result_df = ft.CountAdminLogout().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountAdminLogout(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountAdminLogout(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountAdminLogout(outputCol="testout")
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


def test_count_admin_reject():

    result_df = ft.CountAdminReject().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountAdminReject(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountAdminReject(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountAdminReject(outputCol="testout")
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


def test_count_auth_logout():

    result_df = ft.CountAuthLogout().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountAuthLogout(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountAuthLogout(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountAuthLogout(outputCol="testout")
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


def test_count_validate_accept():

    result_df = (
        ft.CountValidateAccept().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 9

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountValidateAccept(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountValidateAccept(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 9

    result_df = (
        ft.CountValidateAccept(outputCol="testout")
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
    assert result_df.collect()[0][1] == 9


def test_count_validate_reject():

    result_df = (
        ft.CountValidateReject().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountValidateReject(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountValidateReject(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountValidateReject(outputCol="testout")
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


def test_count_visit():

    result_df = ft.CountVisit().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountVisit(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountVisit(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountVisit(outputCol="testout")
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


def test_count_failed():

    result_df = ft.CountFailed().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountFailed(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountFailed(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountFailed(outputCol="testout")
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


def test_count_ou_ams():

    result_df = ft.CountOUAms().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 5

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountOUAms(inputCols=["one", "two"]).get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("one", ufg_df["SM_USERNAME"]).drop(
        "SM_USERNAME"
    )
    test_df = test_df.withColumn("two", ufg_df["SM_RESOURCE"]).drop(
        "SM_RESOURCE"
    )
    result_df = (
        ft.CountOUAms(inputCols=["one", "two"])
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 5

    result_df = (
        ft.CountOUAms(outputCol="testout")
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
    assert result_df.collect()[0][1] == 5


def test_count_ou_cms():

    result_df = ft.CountOUCms().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 4

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountOUCms(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["SM_USERNAME"]).drop(
        "SM_USERNAME"
    )
    result_df = (
        ft.CountOUCms(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 4

    result_df = (
        ft.CountOUCms(outputCol="testout")
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
    assert result_df.collect()[0][1] == 4


def test_count_get():

    result_df = ft.CountGet().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 9

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountGet(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["SM_ACTION"]).drop(
        "SM_ACTION"
    )
    result_df = (
        ft.CountGet(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 9

    result_df = (
        ft.CountGet(outputCol="testout")
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
    assert result_df.collect()[0][1] == 9


def test_count_post():

    result_df = ft.CountPost().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountPost(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["SM_ACTION"]).drop(
        "SM_ACTION"
    )
    result_df = (
        ft.CountPost(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountPost(outputCol="testout")
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


def test_count_http_method():

    result_df = ft.CountHTTPMethod().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 9

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountHTTPMethod(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_ACTION"]).drop(
        "SM_ACTION"
    )
    result_df = (
        ft.CountHTTPMethod(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 9

    result_df = (
        ft.CountHTTPMethod(outputCol="testout")
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
    assert result_df.collect()[0][1] == 9


def test_count_ou_identity():

    result_df = ft.CountOUIdentity().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 5

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountOUIdentity(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_USERNAME"]).drop(
        "SM_USERNAME"
    )
    result_df = (
        ft.CountOUIdentity(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 5

    result_df = (
        ft.CountOUIdentity(outputCol="testout")
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
    assert result_df.collect()[0][1] == 5


def test_count_ou_cred():

    result_df = ft.CountOUCred().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 4

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountOUCred(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["SM_USERNAME"]).drop(
        "SM_USERNAME"
    )
    result_df = (
        ft.CountOUCred(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 4

    result_df = (
        ft.CountOUCred(outputCol="testout")
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
    assert result_df.collect()[0][1] == 4


def test_count_ou_secure_key():

    result_df = ft.CountOUSecurekey().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountOUSecurekey(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_USERNAME"]).drop(
        "SM_USERNAME"
    )
    result_df = (
        ft.CountOUSecurekey(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountOUSecurekey(outputCol="testout")
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


def test_count_portal_mya():

    result_df = ft.CountPortalMya().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountPortalMya(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["SM_RESOURCE"]).drop(
        "SM_RESOURCE"
    )
    result_df = (
        ft.CountPortalMya(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountPortalMya(outputCol="testout")
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


def test_count_portal_myba():

    result_df = ft.CountPortalMyba().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountPortalMyba(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_RESOURCE"]).drop(
        "SM_RESOURCE"
    )
    result_df = (
        ft.CountPortalMyba(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.CountPortalMyba(outputCol="testout")
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


def test_count_records():

    result_df = ft.CountRecords().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 9

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.CountRecords(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["CRA_SEQ"]).drop("CRA_SEQ")
    result_df = (
        ft.CountRecords(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 9

    result_df = (
        ft.CountRecords(outputCol="testout")
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
    assert result_df.collect()[0][1] == 9


def test_user_login_attempts():

    result_df = (
        ft.UserLoginAttempts().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 9

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.UserLoginAttempts(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.UserLoginAttempts(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 9

    result_df = (
        ft.UserLoginAttempts(outputCol="testout")
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
    assert result_df.collect()[0][1] == 9


def test_user_num_of_pw_change():

    result_df = (
        ft.UserNumOfPasswordChange().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.UserNumOfPasswordChange(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_RESOURCE"]).drop(
        "SM_RESOURCE"
    )
    result_df = (
        ft.UserNumOfPasswordChange(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == 0

    result_df = (
        ft.UserNumOfPasswordChange(outputCol="testout")
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
