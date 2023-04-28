# Machine Learning data ingestion API

![Code Coverage](https://img.shields.io/badge/Coverage-99%25-brightgreen.svg)

This is an API that aims to ingest received machine learning problems data and persist it in a database. It was initially built within the scope of [this machine learning problem](https://www.kaggle.com/competitions/santander-customer-transaction-prediction/overview). 

## Stack
- Python
- FastAPI
- MongoDB
- Pydantic
- Pytest

## Installation
### Configure virtual environment and install dependencies
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
Create a `.env` file with the following variables
```
MONGO_INITDB_ROOT_USERNAME=<username>
MONGO_INITDB_ROOT_PASSWORD=<password>
MONGO_INITDB_DATABASE=python_db
DATABASE_URL=mongodb://<username>:<password>@localhost:6000/fastapi?authSource=admin
```

### Create database
```
docker-compose up -d
```

### Run API in develop mode
```
cd src/
uvicorn main:app --reload
```

### Update README test coverage badge
```
export PYTHONPATH=$PWD/src/
python3 -m pytest -vv --cov=src/ --cov-report xml:coverage.xml
readme-cov
```
