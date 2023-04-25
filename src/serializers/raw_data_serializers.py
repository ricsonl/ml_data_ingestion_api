from typing import List, Dict, Any
from decimal import Decimal


def raw_data_entity(data) -> Dict[str, Any]:
    res = {
        'ID_code': str(data['ID_code']),
        'target': data['target']
    }
    for k in data:
        if k.startswith('var_'):
            res[k] = Decimal(str(data.get(k)))
    return res


def raw_data_list_entity(data) -> List[Dict[str, Any]]:
    return [raw_data_entity(reg) for reg in data]
