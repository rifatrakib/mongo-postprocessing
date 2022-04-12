from postprocess.utils import MongoConnectionManager, read_excel_document


def build_field_category_mapper(collection_name, sheet):
    datatype_information = read_excel_document(collection_name, sheet)
    field_category_mapper = []
    for item in datatype_information:
        field_mapper = {
            'field_name': item['field_name'],
            'datatype': 'select' if item['type'] in {'string', 'bool'} else 'range',
        }
        field_category_mapper.append(field_mapper)
    
    return field_category_mapper


def statistical_pipeline_builder(field_category_mapper):
    group_query = {'_id': {}}
    project_query = {'_id': 0}
    for item in field_category_mapper:
        field_name = item['field_name']
        
        if item['datatype'] == 'select':
            group_query['_id'][field_name] = f'${field_name}'
            group_query[field_name] = {'$first': f'${field_name}'}
            project_query[field_name] = 1
        else:
            group_query[f'{field_name}_count'] = {'$sum': 1}
            group_query[f'{field_name}_max'] = {'$max': f'{field_name}'}
            group_query[f'{field_name}_min'] = {'$min': f'{field_name}'}
            group_query[f'{field_name}_avg'] = {'$avg': f'{field_name}'}
            group_query[f'{field_name}_sum'] = {'$sum': f'{field_name}'}
            group_query[f'{field_name}_std_dev'] = {'$stdDevPop': f'{field_name}'}
            
            project_query[field_name] = {
                'count': f'${field_name}_count',
                'max': f'${field_name}_max',
                'min': f'${field_name}_min',
                'average': f'${field_name}_avg',
                'total': f'${field_name}_sum',
                'standard_deviation': f'${field_name}_std_dev',
            }
    
    pipeline = [
        {'$group': group_query},
        {'$project': project_query}
    ]
    return pipeline


def fetch_statistical_data(name, field_category_mapper):
    pipeline = statistical_pipeline_builder(field_category_mapper)
    collection_name = f'{name}_collection'
    with MongoConnectionManager('database', collection_name) as collection:
        data = list(collection.aggregate(pipeline=pipeline, allowDiskUse=True))
    
    return data
