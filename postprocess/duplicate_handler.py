from postprocess.utils import MongoConnectionManager


def build_group_query(field_name, field_id):
    group_query = {
        '_id': f'${field_name}',
        field_name: {'$first': f'${field_name}'},
        'items': {'$addToSet': f'${field_id}'}
    }
    return group_query


def build_project_query(field_name):
    project_query = {
        '_id': 0,
        field_name: 1,
        'count': {'$size': '$items'}
    }
    return project_query


def get_duplicate_names(section_name, field_name, field_id):
    group_query = build_group_query(field_name, field_id)
    project_query = build_project_query(field_name)
    pipeline = [
        {'$group': group_query},
        {'$project': project_query},
        {'$match': {'count': {'$gt': 1}}}
    ]
    
    collection_name = f'{section_name}_collection'
    with MongoConnectionManager('database', collection_name) as collection:
        data = list(collection.aggregate(pipeline=pipeline, allowDiskUse=True))
    
    duplicate_names = [item[field_name] for item in data]
    return duplicate_names


def build_set_query(duplicate_items, field_name, parent_field, location=False):
    if location:
        concat_query = {
            '$concat': [
                f'${field_name}', '-', f'${location}', ' (', f'${parent_field}', ')'
            ]
        }
        else_query = f'${field_name}', '-', f'${location}'
    else:
        concat_query = {'$concat': [f'${field_name}', ' (', f'${parent_field}', ')']}
        else_query = f'${field_name}'
    
    or_conditions = [{'$eq': [f'${field_name}', item]} for item in duplicate_items]
    
    set_query = {
        '$cond': {
            'if': {'$or': or_conditions},
            'then': concat_query,
            'else': else_query
        }
    }
    
    return set_query


def label_fields_setter(section_name, fields):
    set_query = {}
    for key, value in fields.items():
        field_name = value['field_name']
        field_id = value['field_id']
        parent_field = value['parent_field']
        location = value.get('location', None)
        label_field = f'{key}_label'
        
        duplicate_names = get_duplicate_names(section_name, field_name, field_id)
        set_query[label_field] = build_set_query(duplicate_names, field_name, parent_field, location)
    
    return set_query
