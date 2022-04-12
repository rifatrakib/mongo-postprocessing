from .utils import MongoConnectionManager
import pandas as pd


def read_excel_data_as_dict(file_name):
    file_path = f'./static/{file_name}-table.xlsx'
    raw_data = pd.read_excel(file_path, na_filter=False)
    headers = list(raw_data.columns)
    data = list(raw_data[headers].T.to_dict().values())
    filters = {}
    
    for item in data:
        key = item['field']
        filters[key] = item
    
    return filters


def match_query_builder(query_parameters, filters):
    match_stage = {}
    
    for key, value in query_parameters.items():
        if key in filters and filters[key]['input']:
            if filters[key]['type'] in {'range', 'date'}:
                match_stage[key] = {
                '$gte': float(value['min']),
                '$lte': float(value['max'])
            }
            else:
                match_stage[key] = {'$in': value}
    
    return match_stage


def fetch_data_from_collection(collection_name, pipeline):
    with MongoConnectionManager('database', collection_name) as collection:
        data = list(collection.aggregate(pipeline=pipeline, allowDiskUse=True))
    
    return data
