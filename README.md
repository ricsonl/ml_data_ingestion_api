# Machine Learning data ingestion API

![Code Coverage](https://img.shields.io/badge/Coverage-99%25-brightgreen.svg)

This is an API that aims to ingest received machine learning problems data and persist it in a database. It was initially built within the scope of [this machine learning problem](https://www.kaggle.com/competitions/santander-customer-transaction-prediction/overview). 

## Stack
- Python
- FastAPI
- MongoDB
- Pydantic
- Pytest

## Installation (Linux)
### Configure virtual environment and install dependencies
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
Create a `.env` file with the following variables
```
MONGO_INITDB_ROOT_USERNAME=<any_username>
MONGO_INITDB_ROOT_PASSWORD=<any_password>
MONGO_INITDB_DATABASE=<any_database_name>
DATABASE_URL=mongodb://<any_username>:<any_password>@localhost:6000/fastapi?authSource=admin
```

### Create database
- Install [docker-compose](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04)  
- run:
```
docker-compose up -d
```

### Run API in develop mode
```
cd src/
uvicorn main:app --reload
```

## Routes
The path must have at least the name of the target MongoDB collection (could be the table name if we are using SQL databases). On database startup, two collections are created: `train_raw` and `test_raw`, which corresponds to the `train.csv` and `test.csv` [files](https://www.kaggle.com/competitions/santander-customer-transaction-prediction/data). The api doesn't allow creation of new collections.
Some field restrictions for any creation route (POST, PUT):
- `ID_code` must be str
- `target` must be either 0 or 1
- `var_` variables must be numeric

### Create new register
```
POST http://localhost:8000/train_raw

-- Body:
{
  "data": {
    "ID_code": "test_id_1",
    "target": 1,
    "var_0": 7, 
    "var_1": 1.3,
    "var_2": 0.5
  }
}

-- Expected response (201):
{ "message": "Data successfully created" }
```

### Load data from parquet files
```
POST http://localhost:8000/train_raw/massive

-- Body:
{
  "path": "path/to/folder"
}

-- Expected response (201):
{ "message": "Data successfully loaded" }
```

### Update register
```
PUT http://localhost:8000/train_raw

-- Body:
{
  "data": {
    "ID_code": "test_id_1",
    "target": 0,
    "var_0": 8, 
    "var_1": 2.3,
    "var_2": 1.5
  }
}

-- Expected response (201):
{ "message": "Data successfully updated" }
```

### List registers
```
GET http://localhost:8000/train_raw/?limit=2

-- Expected response (200):
{
  "result": [
    {
      "ID_code": "test_id_1",
      "target": 1,
      "var_0": 7, 
      "var_1": 1.3
    },
    {
      "ID_code": "test_id_2",
      "target": 0,
      "var_0": 0.1, 
      "var_1": 1.1
    }
  ]
}
```

### Get specific register
```
GET http://localhost:8000/train_raw/test_id_1

-- Expected response (200):
{
  "result": [
    {
      "ID_code": "test_id_1",
      "target": 1,
      "var_0": 7, 
      "var_1": 1.3
    }
  ]
}
```

### Delete specific register
```
DELETE http://localhost:8000/train_raw/test_id_1

-- Expected response (200):
{ "message": "Data successfully deleted" }
```

### Clear all registers
```
DELETE http://localhost:8000/train_raw/clear

-- Expected response (200):
{ "message": "Collection successfully cleared" }
```

## Update README test coverage badge
```
export PYTHONPATH=$PWD/src/
python3 -m pytest -vv --cov=src/ --cov-report xml:coverage.xml
readme-cov
```
