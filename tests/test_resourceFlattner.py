from caaswx import ResourcesFlattener
from caaswx.caaswx_spark.scripts.loadtestdata import LoadTestData

ingest_data = LoadTestData()

# maximum resource count is 10


def test_single_window_dataframe():
    rf = ResourcesFlattener(max_resource_count=10)
    df = ingest_data.load_test_data("flattener_tests/user_one_window_10.parquet")
    result = rf.transform(df)
    expected_result = ingest_data.load_test_data(
        "flattener_tests/expected_user_one_window_10.parquet"
    )
    assert (
        result.subtract(expected_result).count() == 0
        and expected_result.subtract(result).count() == 0
    )


# user has multiple windows


def test_multiple_windows_dataframe():
    rf = ResourcesFlattener(max_resource_count=5)
    df = ingest_data.load_test_data("flattener_tests/user_multiple_window_5.parquet")
    result = rf.transform(df)
    expected_result = ingest_data.load_test_data(
        "flattener_tests/expected_user_multiple_window_5.parquet"
    )
    assert (
        result.subtract(expected_result).count() == 0
        and expected_result.subtract(result).count() == 0
    )


# window has duplicate resources


def test_single_window_duplicate_resources():
    rf = ResourcesFlattener(max_resource_count=5)
    df = ingest_data.load_test_data("flattener_tests/user_duplicate_resources.parquet")
    result = rf.transform(df)
    expected_result = ingest_data.load_test_data(
        "flattener_tests/expected_user_duplicate_resources.parquet"
    )
    assert (
        result.subtract(expected_result).count() == 0
        and expected_result.subtract(result).count() == 0
    )


# window had duplicate rows that is, the timestamps are also identical to
# check if resources are dropped due to resource count limit


def test_single_window_duplicate_rows():
    rf = ResourcesFlattener(max_resource_count=5)
    df = ingest_data.load_test_data("flattener_tests/user_duplicate_rows.parquet")
    result = rf.transform(df)
    expected_result = ingest_data.load_test_data(
        "flattener_tests/expected_user_duplicate_rows.parquet"
    )
    assert (
        result.subtract(expected_result).count() == 0
        and expected_result.subtract(result).count() == 0
    )


# check basic functionality with max resource count = 5


def test_user_based_grouping():
    rf = ResourcesFlattener(max_resource_count=5)
    df = ingest_data.load_test_data("flattener_tests/user_based_grouping.parquet")
    result = rf.transform(df)
    expected_result = ingest_data.load_test_data(
        "flattener_tests/expected_user_based_grouping.parquet"
    )
    assert (
        result.subtract(expected_result).count() == 0
        and expected_result.subtract(result).count() == 0
    )


# unordered dataset to see if the order of resources is still correct
# based on timestamps


def test_shuffled_dataset():
    rf = ResourcesFlattener(max_resource_count=5)
    df = ingest_data.load_test_data("flattener_tests/user_shuffled_data.parquet")
    result = rf.transform(df)
    expected_result = ingest_data.load_test_data(
        "flattener_tests/expected_user_shuffled_data.parquet"
    )
    assert (
        result.subtract(expected_result).count() == 0
        and expected_result.subtract(result).count() == 0
    )
