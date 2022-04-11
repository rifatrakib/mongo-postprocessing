from postprocess.processor_hub import *

collection_information = [
    {
        'name': 'collection', 'sheet_name': 'delivery',
        'require_processing': True,
        'tasks': {
            'update_datatypes': True, 'validate': True
        }
    },
    {
        'name': 'ignore', 'sheet_name': 'does not matter',
        'require_processing': False,
        'tasks': {
            'update_datatypes': False, 'validate': False
        }
    },
]


def run_updates_on_collections():
    for item in collection_information:
        if item['require_processing']:
            if item['tasks']['update_datatypes']:
                update_collection_field_datatypes(item['name'], item['sheet_name'])
            
            if item['tasks']['validate']:
                validate_collection_field_values(item['name'], item['sheet_name'])


run_updates_on_collections()
