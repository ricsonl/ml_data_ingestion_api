from typing import List
from decimal import Decimal

def raw_data_entity(data) -> dict:
    return {
        'ID_code': str(data['ID_code']),
        'target': data['target'],
        'var': list(map(lambda x: Decimal(str(x)), data['var']))
    }

def raw_data_list_entity(data) -> List:
    return [raw_data_entity(reg) for reg in data]
