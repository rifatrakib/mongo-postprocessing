from advanced_pipelines.utils import read_excel_data_as_dict, fetch_data_from_collection

ignore_values = [0, 0.0, '0', '0.0', '', None]


def populate_filter_with_necessary_data(data, filters):
    filter = []
    for key, value in filters.items():
        filter_data = {
            'field': key,
            'title': value['title'],
            'type': value['type'],
            'open': False,
        }
        
        if data['type'] == 'select':
            filter_data['items'] = data[f'{key}_items']
        elif data['type'] in {'range', 'date'}:
            filter_data['min'] = ''
            filter_data['max'] = ''
        
        filter.append(filter_data)
    
    return filter


def build_conditions(field_name):
    conditional_query = {
        'if': {
            '$and': [{'$ne': [f'${field_name}', value]} for value in ignore_values]
        },
        'then': f'${field_name}',
        'else': '$$REMOVE'
    }
    
    return conditional_query


def get_categorical_fields(filters):
    categorical_fields = set()
    for key, value in filters.items():
        if value['type'] == 'select':
            categorical_fields.add(key)
    
    return categorical_fields


def facet_query_builder(filters):
    facet_query = {}
    categorical_fields = get_categorical_fields(filters)
    
    for key, value in filters.items():
        id_field = value['item_id']
        title_field = value['item_title']
        other_categorical_fields = categorical_fields - {key}
        
        match_query = {key: {'$nin': ignore_values}}
        group_query = {
            '_id': {key: f'${key}'},
            'id': {'$first': f'${id_field}'},
            'title': {'$first': f'${title_field}'},
            'availability': {'$first': True},
            'selected': {'$first': True},
            'visible': {'$first': True},
        }
        
        if id_field != id_field:
            match_query[title_field] = {'$nin': ignore_values}
            group_query['_id'][title_field] = f'${title_field}'
        
        for field in other_categorical_fields:
            group_query[f'{field}_items'] = {
                '$addToSet': {'$cond': build_conditions(field)}}
        
        project_query = {'_id': 0}
        sort_query = {'title': 1}
        
        facet_query[f'{key}_items'] = [
            {'$match': match_query},
            {'$group': group_query},
            {'$project': project_query},
            {'$sort': sort_query}
        ]
    
    return facet_query


def build_data_dependency(name):
    filters = read_excel_data_as_dict(name)
    pipeline = [{'$facet': facet_query_builder(filters)}]
    collection_name = f'{name}_statistics_collection'
    data = fetch_data_from_collection(collection_name, pipeline)
    return data
