from typing import List, Dict, Any

def raw_data_entity(data) -> Dict[str, Any]:
    res = {}
    for k in data:
        if k != '_id':
            res[k] = data.get(k)
    return res

def raw_data_list_entity(data) -> List[Dict[str, Any]]:
    return [raw_data_entity(reg) for reg in data]
