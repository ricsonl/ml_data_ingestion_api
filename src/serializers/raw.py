from typing import List, Dict, Any
from pymongo.typings import _DocumentType
from pymongo.cursor import Cursor

def raw_data_entity(data: _DocumentType) -> Dict[str, Any]:
    res = {}
    for k in data:
        if k != '_id' and data.get(k) != None:
            res[k] = data.get(k)
    return res

def raw_data_list_entity(data: Cursor[_DocumentType]) -> List[Dict[str, Any]]:
    return [raw_data_entity(reg) for reg in data]
