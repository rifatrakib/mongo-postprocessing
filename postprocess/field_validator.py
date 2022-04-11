from postprocess.utils import (
    MongoConnectionManager, read_excel_document, read_text_file_as_list)
from datetime import date


def build_validation_mapper(collection_name, sheet):
    datatype_information = read_excel_document(collection_name, sheet)
    validation_mapper = []
    for item in datatype_information:
        field_mapper = {
            'field_name': item['field_name'],
            'datatype': item['type'],
            'validation_required': item['validation'],
            'domain': item['domain']
        }
        validation_mapper.append(field_mapper)
    
    return validation_mapper


def split_numeric_fields(validation_mapper):
    for item in validation_mapper:
        if item['validation_required']:
            if item['field_name'].endswith('_date'):
                item['num_type'] = 'date'
            elif item['field_name'].endswith('_year'):
                item['num_type'] = 'year'
            else:
                item['num_type'] = 'numeric'
    
    return validation_mapper


def build_switch_query_for_categorical_field(field_name, valid_values):
    branches = []
    for value in valid_values:
        branch = {'case': {'$eq': [f'${field_name}', value]}, 'then': value}
        branches.append(branch)
    
    switch_query = {
        'branches': branches,
        'default': None
    }
    return switch_query


def non_negative_field_validation_query(item):
    field_name = item['field_name']
    condition = {
        'if': {'$lt': [f'${field_name}', 0]},
        'then': None,
        'else': f'${field_name}'
    }
    conditional_query = {'$cond': condition}
    return conditional_query


def defined_range_field_validation_query(item):
    field_name = item['field_name']
    minimum = item['domain'][0]
    maximum = item['domain'][1]
    
    switch_query = {
        'branches': [
            {'case': {'$lt': [f'${field_name}', minimum]}, 'then': minimum},
            {'case': {'$gt': [f'${field_name}', maximum]}, 'then': maximum},
        ],
        'default': f'${field_name}'
    }
    return switch_query


def future_date_validation_query(item, year_only=False):
    field_name = item['field_name']
    current_date = date.today()
    if year_only:
        current_date = current_date.year
    
    set_query = {
        field_name: {
            '$cond': {
                'if': {'$gt': [f'${field_name}', current_date]},
                'then': current_date,
                'else': f'${field_name}'
            }
        }
    }
    return set_query


def categorical_field_validation_query(item):
    field_name = item['field_name']
    valid_values = read_text_file_as_list(field_name)
    switch_query = build_switch_query_for_categorical_field(field_name, valid_values)
    return switch_query


def domain_validation_query(item):
    if item['num_type'] == 'date':
        set_query = future_date_validation_query(item)
    elif item['num_type'] == 'year':
        set_query = future_date_validation_query(item, year_only=True)
    elif item['domain'] == 'non-negative':
        set_query = non_negative_field_validation_query(item)
    elif item['domain'] == 'categorical':
        set_query = categorical_field_validation_query(item)
    elif type(item['domain']) == list:
        set_query = defined_range_field_validation_query(item)
    
    return set_query


def fetch_validation_query(validation_mapper):
    set_query = {}
    for item in validation_mapper:
        domain = item['domain']
        field_name = item['field_name']
        datatype = item['datatype']
        
        if not domain:
            continue
        
        if domain.startswith('[') and domain.endswith(']'):
            domain = [
                float(item) if datatype=='double' else int(item)
                for item in domain[1:-1].split(',')]
            item['domain'] = domain
        
        if item['validation_required']:
            set_query[field_name] = domain_validation_query(item)
    
    return set_query


def build_validation_query(validation_mapper):
    validation_mapper = split_numeric_fields(validation_mapper)
    set_query = fetch_validation_query(validation_mapper)
    return set_query
