from postprocess.processor_hub import *

collection_information = [
    {
        'name': 'collection', 'sheet_name': 'delivery',
        'update_datatypes': True, 'validate': True
    },
    {
        'name': 'ignore', 'sheet_name': 'does not matter',
        'update_datatypes': False, 'validate': True
    },
]


def run_updates_on_collections():
    for item in collection_information:
        if item['update_datatypes']:
            update_collection_field_datatypes(item['name'], item['sheet_name'], item['true_values_only'])
        
        if item['validate']:
            validate_collection_field_values(item['name'], item['sheet_name'])


run_updates_on_collections()
