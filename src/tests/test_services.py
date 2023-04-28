import pytest
from unittest import mock
import pyarrow
from pymongo.errors import DuplicateKeyError, CollectionInvalid, WriteError, OperationFailure
import mongomock
from services.base import BaseService
from services.raw import RawDataService
from schemas.raw import RawDataSchema
from serializers.raw import raw_data_list_entity

@mock.patch('mongomock.database.Database.list_collection_names')
def test_base_validate_collection_name_valid_name(list_collection_names):
    list_collection_names.return_value = ['c_1', 'c_2']
    client = mongomock.MongoClient()

    BaseService.validate_collection_name(client.db, 'c_1')


@mock.patch('mongomock.database.Database.list_collection_names')
def test_base_validate_collection_name_invalid_name(list_collection_names):
    list_collection_names.return_value = ['c_1', 'c_2']
    client = mongomock.MongoClient()

    with pytest.raises(CollectionInvalid) as err:
        BaseService.validate_collection_name(client.db, 'c_3')
    assert str(err.value) == "'collection_name' must be 'c_1' or 'c_2'"


@mock.patch('mongomock.collection.Collection.insert_one')
def test_raw_create_data_valid_input(insert_one):
    client = mongomock.MongoClient()
    collection_name = 'c_1'
    
    data = RawDataSchema(**{
        "ID_code": "fake_id",
        "var_0": 1, 
        "var_1": 1.3,
        "target": None
    })
    function_argument_expected = {
        "ID_code": "fake_id",
        "var_0": 1, 
        "var_1": 1.3,
        "target": None
    }
    RawDataService.create_data(client.db, collection_name, data)
    
    assert 1 == insert_one.call_count
    assert function_argument_expected == insert_one.call_args.args[0]


@mock.patch('mongomock.collection.Collection.insert_one')
def test_raw_create_data_duplicate_key(insert_one):
    client = mongomock.MongoClient()
    collection_name = 'c_1'

    data_1 = {"ID_code": "fake_id"}
    client.db.get_collection(collection_name).insert_many([data_1])

    data_2 = RawDataSchema(**{"ID_code": "fake_id"})
    with pytest.raises(DuplicateKeyError) as err:
        RawDataService.create_data(client.db, collection_name, data_2)

    assert 0 == insert_one.call_count
    assert str(err.value) == "ID_code 'fake_id' already exists in 'c_1' collection"


def test_raw_list_data_no_limit():
    client = mongomock.MongoClient()
    collection_name = 'c_1'
    
    data_1 = {"ID_code": "fake_id_1"}
    data_2 = {"ID_code": "fake_id_2"}
    data_3 = {"ID_code": "fake_id_3"}
    client.db.get_collection(collection_name).insert_many([data_1, data_2, data_3])

    limit = 0
    result_expected = raw_data_list_entity([{'ID_code': 'fake_id_1', 'target': None, '_id': 'testt'},
                                            {'ID_code': 'fake_id_2', 'target': None, '_id': 'testt'},
                                            {'ID_code': 'fake_id_3', 'target': None, '_id': 'testt'}])
    result = RawDataService.list_data(client.db, collection_name, limit)
    
    assert 3==len(result)
    assert result_expected==result


def test_raw_list_data_limit():
    client = mongomock.MongoClient()
    collection_name = 'c_1'
    
    data_1 = {"ID_code": "fake_id_1"}
    data_2 = {"ID_code": "fake_id_2"}
    data_3 = {"ID_code": "fake_id_3"}
    client.db.get_collection(collection_name).insert_many([data_1, data_2, data_3])

    limit = 2
    result_expected = raw_data_list_entity([{'ID_code': 'fake_id_1', 'target': None, '_id': 'testt'},
                                            {'ID_code': 'fake_id_2', 'target': None, '_id': 'testt'}])
    result = RawDataService.list_data(client.db, collection_name, limit)
    
    assert 2==len(result)
    assert result_expected==result


def test_raw_get_data_existing():
    client = mongomock.MongoClient()
    collection_name = 'c_1'
    
    data_1 = {"ID_code": "fake_id"}
    client.db.get_collection(collection_name).insert_one(data_1)

    result_expected = raw_data_list_entity([{'ID_code': 'fake_id', 'target': None, '_id': 'testt'}])
    result = RawDataService.get_data(client.db, collection_name, 'fake_id')
    
    assert 1==len(result)
    assert result_expected==result


def test_raw_get_data_nonexistent():
    client = mongomock.MongoClient()
    collection_name = 'c_1'
    
    data_1 = {"ID_code": "fake_id_1"}
    client.db.get_collection(collection_name).insert_one(data_1)

    result_expected = raw_data_list_entity([])
    result = RawDataService.get_data(client.db, collection_name, 'fake_id_2')
    
    assert 0==len(result)
    assert result_expected==result


@mock.patch('mongomock.collection.Collection.update_one')
def test_raw_update_data_existing_id(update_one):
    client = mongomock.MongoClient()
    collection_name = 'c_1'

    data = {
        "ID_code": "fake_id",
        "var_0": 1, 
        "var_1": 1.3,
        "target": 0
    }
    client.db.get_collection(collection_name).insert_one(data)
    
    new_data = RawDataSchema(**{
        "ID_code": "fake_id",
        "var_0": 2, 
        "var_1": 2.3,
        "target": None
    })

    function_argument_1_expected = {"ID_code": "fake_id"}
    function_argument_2_expected = {
        '$set': {
            "ID_code": "fake_id",
            "var_0": 2, 
            "var_1": 2.3,
            "target": None
        }
    }
    RawDataService.update_data(client.db, collection_name, new_data)

    assert 1 == update_one.call_count
    assert function_argument_1_expected == update_one.call_args.args[0]
    assert function_argument_2_expected == update_one.call_args.args[1]


@mock.patch('mongomock.collection.Collection.update_one')
def test_raw_update_data_nonexistent_id(update_one):
    client = mongomock.MongoClient()
    collection_name = 'c_1'

    data = {
        "ID_code": "fake_id_1",
        "var_0": 1, 
        "var_1": 1.3,
        "target": 0
    }
    client.db.get_collection(collection_name).insert_one(data)

    new_data = RawDataSchema(**{
        "ID_code": "fake_id_2",
        "var_0": 2, 
        "var_1": 2.3,
        "target": 1
    })
    with pytest.raises(WriteError) as err:
        RawDataService.update_data(client.db, collection_name, new_data)

    assert 0 == update_one.call_count
    assert str(err.value) == "ID_code 'fake_id_2' not found in 'c_1' collection"


@mock.patch('mongomock.collection.Collection.delete_one')
def test_raw_delete_data_existing(delete_one):
    client = mongomock.MongoClient()
    collection_name = 'c_1'
    
    data = {
        "ID_code": "fake_id",
        "var_0": 1, 
        "var_1": 1.3,
        "target": 0
    }
    client.db.get_collection(collection_name).insert_one(data)

    function_argument_expected = {'ID_code': 'fake_id'}
    RawDataService.delete_data(client.db, collection_name, 'fake_id')

    assert 1 == delete_one.call_count
    assert function_argument_expected == delete_one.call_args.args[0]


@mock.patch('mongomock.collection.Collection.delete_one')
def test_raw_delete_data_nonexistent(delete_one):
    client = mongomock.MongoClient()
    collection_name = 'c_1'

    with pytest.raises(OperationFailure) as err:
        RawDataService.delete_data(client.db, collection_name, 'fake_id')

    assert 0 == delete_one.call_count
    assert str(err.value) == "ID_code 'fake_id' not found in 'c_1' collection"


@mock.patch('mongomock.collection.Collection.drop')
@mock.patch('mongomock.database.Database.create_collection')
def test_raw_clear_data(create_collection, drop):
    client = mongomock.MongoClient()
    collection_name = 'c_1'

    RawDataService.clear_collection(client.db, collection_name)

    assert 1 == drop.call_count
    assert 1 == create_collection.call_count
    assert collection_name == create_collection.call_args.args[0]


@mock.patch('services.raw.MAX_CHUNKSIZE', 2)
@mock.patch('time.time')
@mock.patch('pyarrow.parquet.read_table')
@mock.patch('mongomock.collection.Collection.insert_many')
def test_raw_load_data(insert_many, read_table, time):
    client = mongomock.MongoClient()
    collection_name = 'c_1'
    path = 'test/path'
    pa_table = pyarrow.Table.from_pylist([
        {'ID_code': 'fake_id_1', 'target': 1},
        {'ID_code': 'fake_id_2', 'target': 1},
        {'ID_code': 'fake_id_3', 'target': 1},
        {'ID_code': 'fake_id_4', 'target': 0},
        {'ID_code': 'fake_id_5', 'target': 0}
    ])
    read_table.return_value = pa_table

    batchs_inserted_expected = [
        [{'ID_code': 'fake_id_1', 'target': 1},{'ID_code': 'fake_id_2', 'target': 1}],
        [{'ID_code': 'fake_id_3', 'target': 1},{'ID_code': 'fake_id_4', 'target': 0}],
        [{'ID_code': 'fake_id_5', 'target': 0}],
    ]
    RawDataService.load_data(client.db, collection_name, path)
    
    assert 3 == insert_many.call_count
    for i, call in enumerate(insert_many.call_args_list):
        assert batchs_inserted_expected[i] == call.args[0]