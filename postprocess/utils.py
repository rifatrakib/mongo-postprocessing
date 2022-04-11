from pymongo import MongoClient
import pandas as pd
import certifi
import os

mongo_uri = os.getenv('MONGO_URI')


class MongoConnectionManager():
    def __init__(self, database, collection):
        self.client = MongoClient(mongo_uri, tlsCAFile=certifi.where())
        self.database = database
        self.collection = collection
    
    def __enter__(self):
        self.database = self.client[self.database]
        self.collection = self.database[self.collection]
        return self.collection
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.client.close()


def update_datatype_inplace(name, set_query):
    collection_name = f'{name}_collection'
    pipeline = [{'$set': set_query}]
    
    with MongoConnectionManager('database', collection_name) as collection:
        collection.update_many({}, pipeline)


def read_excel_document(file_name, sheet):
    file_to_read = f'./static/{file_name}-fields.xlsx'
    excel_data = pd.read_excel(file_to_read, sheet_name=sheet, na_filter=False)
    datatype_information = excel_data.to_dict(orient='records')
    return datatype_information


def read_text_file_as_list(file_name):
    with open(f'./static/{file_name}-list.txt', 'r') as f_obj:
        data = f_obj.read().split(',')
    
    return data
