import pytest
from data.flattener_user_entity import Test_datasets

#window size is 10
def test_single_window_dataframe():

    data_importer = Test_datasets()
    result, ans_df_10 = data_importer.dataset_one_window_10()
    assert (result.subtract(ans_df_10).count() == 0 and ans_df_10.subtract(result).count() == 0)

def test_multiple_windows_dataframe():
    data_importer = Test_datasets()
    result, ans_df_5 = data_importer.dataset_multiple_windows_5()
    assert (result.subtract(ans_df_5).count() == 0 and ans_df_5.subtract(result).count() == 0)

def test_single_window_duplicate_resources():
    data_importer = Test_datasets()
    result, ans_df_5 = data_importer.dataset_single_window_duplicates()
    assert (result.subtract(ans_df_5).count() == 0 and ans_df_5.subtract(result).count() == 0)

def test_single_window_duplicate_rows():
    data_importer = Test_datasets()
    result, ans_df_5 = data_importer.dataset_single_window_duplicate_rows()
    assert (result.subtract(ans_df_5).count() == 0 and ans_df_5.subtract(result).count() == 0)

def test_user_based_grouping():
    data_importer = Test_datasets()
    result, ans_df_5 = data_importer.dataset_check_user_based_grouping()
    assert (result.subtract(ans_df_5).count() == 0 and ans_df_5.subtract(result).count() == 0)

def test_shuffled_dataset():
    data_importer = Test_datasets()
    result, ans_df_5 = data_importer.dataset_shuffled()
    assert (result.subtract(ans_df_5).count() == 0 and ans_df_5.subtract(result).count() == 0)
