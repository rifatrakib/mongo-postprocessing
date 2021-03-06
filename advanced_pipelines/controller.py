from advanced_pipelines.facet_categorical_match_collector import fetch_matched_updates
from advanced_pipelines.statistical_facet_pipelines import fetch_filtered_data
from advanced_pipelines.graph_dependency_builder import build_data_dependency
from advanced_pipelines.utils import update_statistical_collection
import hashlib
import json


def build_statistical_dataset(name):
    query_parameters = {
        # write query here
    }
    data = fetch_filtered_data(query_parameters, name)
    
    prefix = f'{name} - '
    query_hash = hashlib.md5((json.dumps(query_parameters)).encode('utf-8')).hexdigest()
    file_name = prefix + query_hash
    with open(f'.static/responses/builder_dataset/{file_name}.json', 'w') as writer:
        writer.write(json.dumps(data))
    
    update_statistical_collection(data, name)


def collect_matching_statistical_data(name):
    query_parameters = {
        # write query here
    }
    data = fetch_matched_updates(query_parameters, name)
    
    prefix = f'{name} - '
    query_hash = hashlib.md5((json.dumps(query_parameters)).encode('utf-8')).hexdigest()
    file_name = prefix + query_hash
    with open(f'.static/responses/queried_stats/{file_name}.json', 'w') as writer:
        writer.write(json.dumps(data))


def collect_graph_data(name):
    data = build_data_dependency(name)
    
    file_name = f'{name}-graph-data'
    with open(f'.static/responses/graph_data/{file_name}.json', 'w') as writer:
        writer.write(json.dumps(data))
