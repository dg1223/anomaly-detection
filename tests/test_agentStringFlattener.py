from data.dataagentstringflattener import DataAgentStringFlattener


# if parser is now working, run test_1, edit code in AgentStringFlattener to activate parser and uncomment block below
def test_1():
    data_importer = DataAgentStringFlattener()
    result, ans_1_data = data_importer.ds1_base()

    # content test
    assert result.subtract(ans_1_data).count() == 0

    # schema test
    assert result.schema == ans_1_data.schema

    # row test
    assert result.count() == ans_1_data.count()


# if parser is not working, run test_2, edit code in AgentStringFlattener to deactivate parser
def test_2():
    data_importer = DataAgentStringFlattener()
    result, ans_1_data = data_importer.ds2_base()

    # content test
    assert result.subtract(ans_1_data).count() == 0

    # schema test
    assert result.schema == ans_1_data.schema

    # row test
    assert result.count() == ans_1_data.count()
