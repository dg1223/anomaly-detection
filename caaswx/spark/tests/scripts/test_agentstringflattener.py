import pytest
from data_agentstringflattener import agentflattener_datasets 

def test_1():
  data_importer = agentflattener_datasets()
  result, ans_1_data = data_importer.ds1_base()
  assert(result.subtract(ans_1_data).count() == 0 and ans_1_data.subtract(result).count() == 0)
