import os
from typing import Dict, Any
from bson.decimal128 import Decimal128


def list_parquet_files(path: str) -> str:
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)) and file.endswith('.parquet'):
            yield file


def convert_vars_decimal(data: Dict[str, Any]) -> Dict[str, Any]:
    return dict(map(lambda d: (d[0], Decimal128(str(d[1]))) if d[0].startswith('var_') else d, data.items()))