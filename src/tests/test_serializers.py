from serializers.raw import raw_data_entity, raw_data_list_entity

def test_raw_data_entity():
    data = {'any_field_1': 'any_value_1', 'any_field_2': None, '_id': 'mongo_id_2'}
    expected_result = {'any_field_1': 'any_value_1'}
    result = raw_data_entity(data)

    assert expected_result == result


def test_raw_data_list_entity():
    data = [
        {'any_field_1': 'any_value_1', 'any_field_2': 'any_value_2', '_id': 'mongo_id_1'},
        {'any_field_1': 'any_value_1', 'any_field_2': None, '_id': 'mongo_id_2'},
        {'any_field_1': 'any_value_1', 'any_field_2': None},
        {'any_field_1': 'any_value_1'},
        {'any_field_1': 'any_value_1', '_id': 'mongo_id_5'}
    ]
    expected_result = [
        {'any_field_1': 'any_value_1', 'any_field_2': 'any_value_2',},
        {'any_field_1': 'any_value_1'},
        {'any_field_1': 'any_value_1'},
        {'any_field_1': 'any_value_1'},
        {'any_field_1': 'any_value_1'}
    ]
    result = raw_data_list_entity(data)

    assert expected_result == result
