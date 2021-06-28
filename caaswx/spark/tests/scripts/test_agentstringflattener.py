import pytest
from dataAgentstringflattener import agentflattener_datasets 

def test_1():
  data_importer = agentflattener_datasets()
  result, ans_df = data_importer.ds1_base()
  # print(result, ans_1_data)

  assert(result == ans_df)
