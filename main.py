from advanced_pipelines.controller import *
from postprocess.controller import *

collection_information = [
    {
        'name': 'collection', 'sheet_name': 'delivery',
        'require_processing': True, 'build_pipelines': True,
        'processing_tasks': {
            'update_datatypes': True, 'validate': True,
            'labelleing': True, 'stat_builder': True,
        },
        'pipeline_tasks': {
            'build_stat_collection': True,
            'collect_dependency_data': True,
            'graph_data_collection': True,
        }
    },
    {
        'name': 'ignore', 'sheet_name': 'does not matter',
        'require_processing': False,
        'tasks': {
            'update_datatypes': False, 'validate': False,
            'labelleing': False, 'stat_builder': False,
        },
    },
]


def run_updates_on_collections():
    for item in collection_information:
        if item.get('require_processing', False):
            if item['processing_tasks']['update_datatypes']:
                update_collection_field_datatypes(item['name'], item['sheet_name'])
            
            if item['processing_tasks']['validate']:
                validate_collection_field_values(item['name'], item['sheet_name'])
            
            if item['processing_tasks']['labelleing']:
                resolve_collection_duplicate_data(item['name'])
            
            if item['processing_tasks']['stat_builder']:
                statistical_collection_builder(item['name'], item['sheet_name'])
        
        if item.get('build_pipelines', False):
            if item['pipeline_tasks']['build_stat_collection']:
                build_statistical_dataset(item['name'])
        
        if item.get('build_pipelines', False):
            if item['pipeline_tasks']['collect_dependency_data']:
                collect_matching_statistical_data(item['name'])
        
        if item.get('build_pipelines', False):
            if item['pipeline_tasks']['graph_data_collection']:
                collect_graph_data(item['name'])


run_updates_on_collections()
