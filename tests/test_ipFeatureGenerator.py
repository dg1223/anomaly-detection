from pyspark.sql.session import SparkSession
from src.caaswx.spark.transformers import (
    IPFeatureGenerator,
)
from src.caaswx.spark.utils import load_test_data

spark = SparkSession.builder.getOrCreate()


def test_content():
    """
    Tests if the data in the results is the same as the data in the
    expected result.
    """
    test_df = load_test_data(
        "data", "parquet_data", "ip_feature_generator_tests", "data_df.parquet"
    )

    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "transformers_tests",
        "ans_df" ".parquet",
    )

    fg = IPFeatureGenerator()
    result = fg.transform(test_df)

    result_assert = result.subtract(ans_1_data).count()
    ans_assert = ans_1_data.subtract(result).count()

    # content test
    assert result_assert == 0 and ans_assert == 0

    # row test
    assert result.count() == ans_1_data.count()


def test_empty_data():
    """
    Tests transformer behaviour with an empty dataset.
    """
    test_df = load_test_data(
        "data",
        "parquet_data",
        "ip_feature_generator_tests",
        "data_empty_df.parquet",
    )

    fg = IPFeatureGenerator()
    result = fg.transform(test_df)

    assert result.count() == 0
