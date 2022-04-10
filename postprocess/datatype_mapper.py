from postprocess.utils import MongoConnectionManager
import pandas as pd

operator_mapper = {
    'string': '$toString',
    'int': '$toInt',
    'double': '$toDouble',
    'bool': '$toBool'
}


def read_excel_document(collection_name, sheet, true_fields_only):
    file_to_read = f'./static/{collection_name}-fields.xlsx'
    excel_data = pd.read_excel(file_to_read, sheet_name=sheet)
    
    if true_fields_only:
        excel_data = excel_data[excel_data['cast']]
    
    datatype_information = excel_data.to_dict(orient='records')
    return datatype_information


def build_datatype_mapper(collection_name, sheet, true_fields_only=False):
    datatype_information = read_excel_document(collection_name, sheet, true_fields_only)
    mapper = []
    for item in datatype_information:
        field_mapper = {
            'field_name': item['field_name'],
            'operator': operator_mapper[item['type']],
            'casting_required': item['cast']
        }
        mapper.append(field_mapper)
    
    return mapper


def build_set_query(mapper):
    set_query = {}
    for field_name, operator in mapper.items():
        set_query[field_name] = {operator: f'${field_name}'}
    
    return set_query


def update_datatype_inplace(name, set_query):
    collection_name = f'{name}_collection'
    pipeline = [{'$set': set_query}]
    
    with MongoConnectionManager('database', collection_name) as collection:
        collection.update_many({}, pipeline)
