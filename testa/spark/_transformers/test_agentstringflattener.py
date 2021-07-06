from testa.generateTestData.dataAgentstringflattener import agentflattener_datasets


# if parser is now working run test_1, edit code in agentstringflattener to activate parser
# test 1 will fail in current config
'''
def test_1():
  data_importer = agentflattener_datasets()
  result, ans_1_data = data_importer.ds1_base()
  # print(result, ans_1_data)
  # content test
  assert(result.subtract(ans_1_data).count() == 0)

  # schema test
  assert(result.schema == ans_1_data.schema)

  # row test
  assert(result.count() == ans_1_data.count())
'''

# if parser is not working, run test_2, edit code in agentstringflattener to deactivate parser


def test_2():

  data_importer = agentflattener_datasets()
  result, ans_1_data = data_importer.ds2_base()

  # content test
  assert(result.subtract(ans_1_data).count() == 0)

  # schema test
  assert(result.schema == ans_1_data.schema)

  # row test
  assert(result.count() == ans_1_data.count())

