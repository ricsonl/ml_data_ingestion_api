from unittest import mock
from fastapi.testclient import TestClient
from fastapi import status
from main import app
from pymongo.errors import CollectionInvalid

fastapi_client=TestClient(app=app)

def test_health_check():
    response_expected = {'message': 'Welcome'}
    response = fastapi_client.get('/')

    assert status.HTTP_200_OK == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.create_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_create_data_ok(__getattr__, create_data, validate_collection_name):
    collection_name = 'c_1'
    body = {
        "data": {"ID_code": "test"}
    }
    response_expected = {'message': 'Data successfully created'}
    response = fastapi_client.post(f'/{collection_name}/', json=body)

    assert 1 == validate_collection_name.call_count
    assert 1 == create_data.call_count
    assert collection_name == create_data.call_args.args[1]
    assert status.HTTP_201_CREATED == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.create_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_create_data_validate_collection_name_failed(__getattr__, create_data, validate_collection_name):
    error_message = 'message'
    validate_collection_name.side_effect = CollectionInvalid(error_message)

    collection_name = 'c_1'
    body = {
        "data": {"ID_code": "test"}
    }
    response_expected = {'detail': error_message}
    response = fastapi_client.post(f'/{collection_name}/', json=body)

    assert 1 == validate_collection_name.call_count
    assert 0 == create_data.call_count
    assert status.HTTP_400_BAD_REQUEST == response.status_code
    assert response_expected == response.json()

    
@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.list_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_list_data_ok(__getattr__, list_data, validate_collection_name):
    result = [{'some': 'data'}]
    list_data.return_value = result

    collection_name = 'c_1'
    response_expected = {'result': result}
    response = fastapi_client.get(f'/{collection_name}/')

    assert 1 == validate_collection_name.call_count
    assert 1 == list_data.call_count
    assert collection_name == list_data.call_args.args[1]
    assert status.HTTP_200_OK == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.list_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_list_data_with_limit(__getattr__, list_data, validate_collection_name):
    result = [{'some': 'data'}]
    list_data.return_value = result

    collection_name = 'c_1'
    limit = 3
    response_expected = {'result': result}
    response = fastapi_client.get(f'/{collection_name}/?limit={limit}')

    assert 1 == validate_collection_name.call_count
    assert 1 == list_data.call_count
    assert collection_name == list_data.call_args.args[1]
    assert limit == list_data.call_args.args[2]
    assert status.HTTP_200_OK == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.list_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_list_data_validate_collection_name_failed(__getattr__, list_data, validate_collection_name):
    error_message = 'message'
    validate_collection_name.side_effect = CollectionInvalid(error_message)

    collection_name = 'c_1'
    response_expected = {'detail': error_message}
    response = fastapi_client.get(f'/{collection_name}/')

    assert 1 == validate_collection_name.call_count
    assert 0 == list_data.call_count
    assert status.HTTP_400_BAD_REQUEST == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.get_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_get_data_ok(__getattr__, get_data, validate_collection_name):
    result = {'some': 'data'}
    get_data.return_value = result

    collection_name = 'c_1'
    id = 'id_1'
    response_expected = {'result': result}
    response = fastapi_client.get(f'/{collection_name}/{id}')

    assert 1 == validate_collection_name.call_count
    assert 1 == get_data.call_count
    assert collection_name == get_data.call_args.args[1]
    assert id == get_data.call_args.args[2]
    assert status.HTTP_200_OK == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.get_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_get_data_validate_collection_name_failed(__getattr__, get_data, validate_collection_name):
    error_message = 'message'
    validate_collection_name.side_effect = CollectionInvalid(error_message)

    collection_name = 'c_1'
    id = 'id_1'
    response_expected = {'detail': error_message}
    response = fastapi_client.get(f'/{collection_name}/{id}')

    assert 1 == validate_collection_name.call_count
    assert 0 == get_data.call_count
    assert status.HTTP_400_BAD_REQUEST == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.update_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_update_data_ok(__getattr__, update_data, validate_collection_name):
    collection_name = 'c_1'
    body = {
        "data": {"ID_code": "test"}
    }
    response_expected = {'message': 'Data successfully updated'}
    response = fastapi_client.put(f'/{collection_name}/', json=body)

    assert 1 == validate_collection_name.call_count
    assert 1 == update_data.call_count
    assert collection_name == update_data.call_args.args[1]
    assert status.HTTP_201_CREATED == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.update_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_update_data_validate_collection_name_failed(__getattr__, update_data, validate_collection_name):
    error_message = 'message'
    validate_collection_name.side_effect = CollectionInvalid(error_message)

    collection_name = 'c_1'
    body = {
        "data": {"ID_code": "test"}
    }
    response_expected = {'detail': error_message}
    response = fastapi_client.put(f'/{collection_name}/', json=body)

    assert 1 == validate_collection_name.call_count
    assert 0 == update_data.call_count
    assert status.HTTP_400_BAD_REQUEST == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.clear_collection')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_clear_collection_ok(__getattr__, clear_collection, validate_collection_name):
    collection_name = 'c_1'
    response_expected = {'message': 'Collection successfully cleared'}
    response = fastapi_client.delete(f'/{collection_name}/clear')

    assert 1 == validate_collection_name.call_count
    assert 1 == clear_collection.call_count
    assert collection_name == clear_collection.call_args.args[1]
    assert status.HTTP_200_OK == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.clear_collection')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_clear_collection_validate_collection_name_failed(__getattr__, clear_collection, validate_collection_name):
    error_message = 'message'
    validate_collection_name.side_effect = CollectionInvalid(error_message)

    collection_name = 'c_1'
    response_expected = {'detail': error_message}
    response = fastapi_client.delete(f'/{collection_name}/clear')

    assert 1 == validate_collection_name.call_count
    assert 0 == clear_collection.call_count
    assert status.HTTP_400_BAD_REQUEST == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.delete_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_delete_data_ok(__getattr__, delete_data, validate_collection_name):
    collection_name = 'c_1'
    id = 'id_1'
    response_expected = {'message': 'Data successfully deleted'}
    response = fastapi_client.delete(f'/{collection_name}/{id}')

    assert 1 == validate_collection_name.call_count
    assert 1 == delete_data.call_count
    assert collection_name == delete_data.call_args.args[1]
    assert id == delete_data.call_args.args[2]
    assert status.HTTP_200_OK == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.delete_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_delete_data_validate_collection_name_failed(__getattr__, delete_data, validate_collection_name):
    error_message = 'message'
    validate_collection_name.side_effect = CollectionInvalid(error_message)

    collection_name = 'c_1'
    id = 'id_1'
    response_expected = {'detail': error_message}
    response = fastapi_client.delete(f'/{collection_name}/{id}')

    assert 1 == validate_collection_name.call_count
    assert 0 == delete_data.call_count
    assert status.HTTP_400_BAD_REQUEST == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.load_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_load_data_ok(__getattr__, load_data, validate_collection_name):
    collection_name = 'c_1'
    body = {
        "path": "some/path"
    }
    response_expected = {'message': 'Data successfully loaded'}
    response = fastapi_client.post(f'/{collection_name}/massive', json=body)

    assert 1 == validate_collection_name.call_count
    assert 1 == load_data.call_count
    assert collection_name == load_data.call_args.args[1]
    assert status.HTTP_201_CREATED == response.status_code
    assert response_expected == response.json()


@mock.patch('services.raw.RawDataService.validate_collection_name')
@mock.patch('services.raw.RawDataService.load_data')
@mock.patch('database.connection.MongoManager.__getattr__')
def test_raw_load_data_validate_collection_name_failed(__getattr__, load_data, validate_collection_name):
    error_message = 'message'
    validate_collection_name.side_effect = CollectionInvalid(error_message)

    collection_name = 'c_1'
    body = {
        "path": "some/path"
    }
    response_expected = {'detail': error_message}
    response = fastapi_client.post(f'/{collection_name}/massive', json=body)

    assert 1 == validate_collection_name.call_count
    assert 0 == load_data.call_count
    assert status.HTTP_400_BAD_REQUEST == response.status_code
    assert response_expected == response.json()