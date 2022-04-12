from .utils import MongoConnectionManager
import pandas as pd


def update_datatype_inplace(name, set_query):
    collection_name = f'{name}_collection'
    pipeline = [{'$set': set_query}]
    
    with MongoConnectionManager('database', collection_name) as collection:
        collection.update_many({}, pipeline)


def rebuild_statistical_collection(name, data):
    collection_name = f'{name}_stat_collection'
    
    with MongoConnectionManager('database', collection_name) as collection:
        collection.delete_many({})
        collection.insert_many(data)


def read_excel_document(file_name, sheet):
    file_to_read = f'./static/{file_name}-fields.xlsx'
    excel_data = pd.read_excel(file_to_read, sheet_name=sheet, na_filter=False)
    datatype_information = excel_data.to_dict(orient='records')
    return datatype_information


def read_text_file_as_list(file_name):
    with open(f'./static/{file_name}-list.txt', 'r') as f_obj:
        data = f_obj.read().split(',')
    
    return data
