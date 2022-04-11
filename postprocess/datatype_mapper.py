from postprocess.utils import MongoConnectionManager, read_excel_document

operator_mapper = {
    'string': '$toString',
    'int': '$toInt',
    'double': '$toDouble',
    'bool': '$toBool'
}


def build_datatype_mapper(collection_name, sheet):
    datatype_information = read_excel_document(collection_name, sheet)
    datatype_mapper = []
    for item in datatype_information:
        field_mapper = {
            'field_name': item['field_name'],
            'operator': operator_mapper[item['type']],
            'casting_required': item['cast']
        }
        datatype_mapper.append(field_mapper)
    
    return datatype_mapper


def build_type_conversion_query(datatype_mapper):
    set_query = {}
    for item in datatype_mapper:
        field_name = item['field_name']
        operator = item['operator']
        casting_required = item['cast']
        if casting_required:
            set_query[field_name] = {operator: f'${field_name}'}
    
    return set_query
