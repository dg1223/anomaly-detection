import pytest
from test_caaswx.generateTestData.flattener_user_entity import Test_datasets

#maximum resource count is 10
def test_single_window_dataframe():

    data_importer = Test_datasets()
    result, ans_df_10 = data_importer.dataset_one_window_10()
    assert (result.subtract(ans_df_10).count() == 0 and ans_df_10.subtract(result).count() == 0)

#user has multiple windows
def test_multiple_windows_dataframe():
    data_importer = Test_datasets()
    result, ans_df_5 = data_importer.dataset_multiple_windows_5()
    assert (result.subtract(ans_df_5).count() == 0 and ans_df_5.subtract(result).count() == 0)

#window has duplicate resources
def test_single_window_duplicate_resources():
    data_importer = Test_datasets()
    result, ans_df_5 = data_importer.dataset_single_window_duplicates()
    assert (result.subtract(ans_df_5).count() == 0 and ans_df_5.subtract(result).count() == 0)

#window had duplicate rows that is, the timestamps are also identical to check if resources are dropped due to resource count limit
def test_single_window_duplicate_rows():
    data_importer = Test_datasets()
    result, ans_df_5 = data_importer.dataset_single_window_duplicate_rows()
    assert (result.subtract(ans_df_5).count() == 0 and ans_df_5.subtract(result).count() == 0)

#check basic functionality with max resource count = 5
def test_user_based_grouping():
    data_importer = Test_datasets()
    result, ans_df_5 = data_importer.dataset_check_user_based_grouping()
    assert (result.subtract(ans_df_5).count() == 0 and ans_df_5.subtract(result).count() == 0)

#unordered dataset to see if the order of resources is still correct based on timestamps
def test_shuffled_dataset():
    data_importer = Test_datasets()
    result, ans_df_5 = data_importer.dataset_shuffled()
    assert (result.subtract(ans_df_5).count() == 0 and ans_df_5.subtract(result).count() == 0)
