from src.caaswx.spark._transformers.cnextractor import CnExtractor
from src.caaswx.spark.scripts.loadWriteParquet import load_parquet


def test_cnextractor():
    obj = CnExtractor()
    test_df = load_parquet(
        "data",
        "parquet_data",
        "cn_extractor_tests",
        "SM_USERNAME_2_examples.parquet",
    )
    answer_df = load_parquet(
        "data",
        "parquet_data",
        "cn_extractor_tests",
        "expected_SM_USERNAME_2_examples.parquet",
    )
    test_df.show()
    answer_df.show()
    result_df = obj.transform(test_df)
    sub1 = result_df.subtract(answer_df)
    sub2 = answer_df.subtract(result_df)

    assert bool(sub1.head(1)) is bool(sub2.head(1)) is False
