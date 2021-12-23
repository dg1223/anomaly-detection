from pyspark.sql.session import SparkSession
from src.caaswx.spark.transformers import (
    UserFeatureGenerator,
)
from src.caaswx.spark.utils import load_test_data

spark = SparkSession.builder.getOrCreate()


def test_num_rows():

    test_df = load_test_data(
        "data", "parquet_data", "user_feature_generator_tests", "data.parquet"
    )

    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "user_feature_generator_tests",
        "ans.parquet",
    )

    fg = UserFeatureGenerator()
    result = fg.transform(test_df)

    """
    Tests if number of rows are the same between the results and expected
    result.
    """
    # row test
    assert result.count() == ans_1_data.count()

    """
    Tests if the data in the results is the same as the data in the
    expected result.
    """
    result_assert = result.subtract(ans_1_data).count()
    ans_assert = ans_1_data.subtract(result).count()

    # content test
    assert result_assert == 0 and ans_assert == 0


def test_empty_data():
    """
    Tests transformer behaviour with an empty dataset.
    """
    test_df = load_test_data(
        "data",
        "parquet_data",
        "user_feature_generator_tests",
        "data_empty_df.parquet",
    )

    fg = UserFeatureGenerator()
    result = fg.transform(test_df)

    assert result.count() == 0
