from pyspark.sql.session import SparkSession

from src.caaswx.spark.transformers import SessionFeatureGenerator
from src.caaswx.spark.utils import load_test_data

spark = SparkSession.builder.getOrCreate()


def test_content():
    """
    Tests if the data in the results is the same as the data in the
    expected result. (Same content and number of rows)
    """
    test_df = load_test_data(
        "data",
        "parquet_data",
        "session_feature_generator_tests",
        "data_df.parquet",
    )

    ans_1_data = load_test_data(
        "data",
        "parquet_data",
        "session_feature_generator_tests",
        "ans_data.parquet",
    )

    fg = SessionFeatureGenerator()
    result = fg.transform(test_df)

    # content test
    assert result.subtract(ans_1_data).count() == 0

    # row test
    assert result.count() == ans_1_data.count()


def test_empty_data():
    """
    Tests transformer behaviour with an empty dataset.
    """
    test_df = load_test_data(
        "data",
        "parquet_data",
        "session_feature_generator_tests",
        "data_empty_df.parquet",
    )

    fg = SessionFeatureGenerator()
    result = fg.transform(test_df)

    assert result.count() == 0
