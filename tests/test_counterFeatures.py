from pyspark.sql.functions import (
    col,
    when,
    regexp_extract,
    collect_list,
)
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructType,
    StructField,
)
import src.caaswx.spark.features as ft
from src.caaswx.spark.base import (
    GroupbyTransformer,
    CounterFeature,
    DistinctCounterFeature,
    ArrayDistinctFeature,
    ArrayRemoveFeature,
    SizeArrayRemoveFeature,
)
from src.caaswx.spark.utils import (
    HasTypedInputCol,
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