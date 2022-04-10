from postprocess.processor_hub import update_collection_field_datatypes

collection_information = [
    {
        'name': 'collection', 'sheet_name': 'delivery',
        'update_datatypes': True, 'true_values_only': True
    },
    {
        'name': 'ignore', 'sheet_name': 'does not matter',
        'update_datatypes': False, 'true_values_only': True
    },
]


def run_updates_on_collections():
    for item in collection_information:
        if item['update_datatypes']:
            update_collection_field_datatypes(item['name'], item['sheet_name'], item['true_values_only'])


run_updates_on_collections()
