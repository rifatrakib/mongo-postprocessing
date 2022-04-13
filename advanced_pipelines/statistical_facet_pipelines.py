from advanced_pipelines.utils import (
    read_excel_data_as_dict, fetch_data_from_collection,
    match_query_builder)


def build_facet_stages_for_categorical_field(item_filter):
    item_id = item_filter['item_id']
    item_title = item_filter['item_title']
    
    item_match = {}
    item_group = {}
    
    if item_id == item_title:
        item_match[item_id] = {'$ne': None}
        item_group['_id'] = {item_id: f'${item_title}'}
    else:
        item_match[item_id] = {'$ne': None}
        item_match[item_title] = {'$ne': None}
        item_group['_id'] = {
            item_id: f'${item_id}',
            item_title: f'${item_title}'}
    
    item_group['title'] = {'$first': f'${item_title}'}
    item_group['count'] = {'$sum': 1}
    
    item_project = {'_id': 0}
    field_facet_stage = [
        {'$match': item_match},
        {'$group': item_group},
        {'$project': item_project},
    ]
    
    return field_facet_stage


def build_facet_stages_for_range_field(item_filter, bucket_size=10):
    item_field = item_filter['field']
    item_match = {item_field: {'$gte': 0}}
    
    item_bucket_auto = {
        'groupBy': f'${item_field}',
        'buckets': bucket_size,
        'output': {'count': {'$sum': 1}},
        'granularity': 'R10'
    }
    
    item_set = {'title': {'$concat': [
        {'$toString': '$_id.min'}, ' - ', {'$toString': '$_id.max'}
    ]}}
    
    item_project = {'_id': 0}
    field_facet_stage = [
        {'$match': item_match},
        {'$bucketAuto': item_bucket_auto},
        {'$set': item_set},
        {'$project': item_project}
    ]
    
    return field_facet_stage


def facet_query_builder(filters):
    facet_stage = {}
    
    for key, value in filters.items():
        if value['output'] == True:
            if value['type'] in {'select', 'radio'}:
                facet_stage[key] = build_facet_stages_for_categorical_field(value)
            else:
                facet_stage[key] = build_facet_stages_for_range_field(value)
    
    return facet_stage


def process_statistical_data(data):
    processed_data = {}
    
    for key, value in data.items():
        item_data = {}
        for item in value:
            item_title = item['title']
            item_count = item['count']
            item_data[item_title] = item_count
        
        processed_data[key] = item_data
    
    return processed_data


def fetch_filtered_data(query_parameters, name):
    """need to pass query_parameters as the following format
    {
        "county_number":["3","30"], # any categorical field types
        "purchase_price":{"min": 1000,"max": 1200000}, # any ranged field types
    }
    """
    filters = read_excel_data_as_dict(name)
    match_query = match_query_builder(query_parameters, filters)
    facet_query = facet_query_builder(filters)
    
    collection_name = f'{name}_collection'
    pipeline = [
        {'$match': match_query},
        {'$facet': facet_query}
    ]
    
    data = fetch_data_from_collection(collection_name, pipeline)[0]
    result = process_statistical_data(data)
    
    return result
