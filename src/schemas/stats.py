# from pydantic import BaseModel, StrictInt, StrictStr, Extra
# import numbers

# class StatsItemSchema(BaseModel):
#     count: StrictInt
#     mean: numbers.Number
#     std: numbers.Number
#     min: numbers.Number
#     p_25: numbers.Number
#     p_50: numbers.Number
#     p_75: numbers.Number
#     max: numbers.Number

#     class Config:
#         orm_mode = True

# class StatsSchema(BaseModel):
#     field_name: StrictStr
#     stats: StatsItemSchema

#     class Config:
#         orm_mode = True