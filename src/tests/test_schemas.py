import pytest
from unittest import mock
from schemas.raw import RawDataSchema

def test_raw_data_schema_validation_ok():
    RawDataSchema(ID_code='test_1', target=1, var_0=1, var_1=1.3, var_2=None)
    RawDataSchema(ID_code='test_2', target=0, var_0=1, var_1=1.3, var_2=None)
    RawDataSchema(ID_code='test_3', target=None)
    RawDataSchema(ID_code='test_4', target=1)
    RawDataSchema(ID_code=None, target=None)
    RawDataSchema()


def test_raw_data_schema_validation_type_error():
    with pytest.raises(ValueError):
        RawDataSchema(ID_code=1)
    with pytest.raises(ValueError):
        RawDataSchema(target="0")
    with pytest.raises(ValueError):
        RawDataSchema(var_0=[])


def test_raw_data_schema_validation_variable_not_listed():
    with pytest.raises(ValueError):
        RawDataSchema(intruder=1)


def test_raw_data_schema_validation_target_not_allowed():
    with pytest.raises(ValueError):
        RawDataSchema(target=2)


@mock.patch('schemas.raw.MAX_VARS', 1)
def test_raw_data_schema_validation_num_vars_exceeded():
    with pytest.raises(ValueError):
        RawDataSchema(var_0=1, var_1=1)