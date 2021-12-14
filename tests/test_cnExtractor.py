from src.caaswx.spark.transformers import CnExtractor
from src.caaswx.spark.utils import load_parquet


def test_cnextractor():
    """
    Tests if the data in the results is the same as the data in the
    expected results.
    """
    obj = CnExtractor()
    test_df = load_parquet(
        "data",
        "parquet_data",
        "cn_extractor_tests",
        "SM_USERNAME_2_examples.parquet",
    )
    ans_1_data = load_parquet(
        "data",
        "parquet_data",
        "cn_extractor_tests",
        "expected_SM_USERNAME_2_examples.parquet",
    )
    result = obj.transform(test_df)

    result_assert = result.subtract(ans_1_data).count()
    ans_assert = ans_1_data.subtract(result).count()

    # content test
    assert (
        result_assert == 0 and ans_assert == 0
    )


def test_empty_data():
    """
    Tests transformer behaviour with an empty dataset.
    """
    obj = CnExtractor()
    test_df = load_parquet(
        "data",
        "parquet_data",
        "cn_extractor_tests",
        "data_empty_df.parquet",
    )
    result_df = obj.transform(test_df)

    assert result_df.count() == 0
