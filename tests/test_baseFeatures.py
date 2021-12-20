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


# get data and run transformer
ufg_df = load_test_data(
    "data", "parquet_data", "user_feature_generator_tests", "data.parquet",
)

# ans_df = load_test_data(
#     "data",
#     "parquet_data",
#     "user_feature_generator_tests",
#     "ans.parquet",
# )
# df = spark.read.parquet("../data/parquet_data/user_feature_generator_tests/data.parquet")
# fg = ft.CountAuthAccept().get_transformer(["CN"])
# result_df = fg.transform(df)
# result_df.show()

# check cols for accuracy (asserts)


def test_counter_feature():

    result_df = (
        ft.CountAuthAccept(outputCol="testout")
        .get_transformer(["CN"])
        .transform(ufg_df)
    )

    """
    Tests number of rows of Dataframe.
    """
    assert result_df.count() == 1

    """
    Tests feature Core Functionality
    """
    assert result_df.collect()[0][1] == 0

    """
    Tests Constructor Output Col Name 
    """
    assert "testout" in result_df.columns

    """
    Test catching incorrect inputCol
    """
    with raises(ValueError):
        ft.CountAuthAccept(inputCol="testout").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    """
    Tests InputCol is working with different name
    """
    test_df = ufg_df.withColumn("testout", ufg_df["SM_EVENTID"]).drop(
        "SM_EVENTID"
    )
    result_df = (
        ft.CountAuthAccept(inputCol="testout")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    assert result_df.collect()[0][1] == 0
