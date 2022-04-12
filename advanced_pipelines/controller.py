from sys import prefix
from advanced_pipelines.facet_pipelines import fetch_filtered_data
import hashlib
import json


def collect_statistical_data(name):
    query_parameters = {
        # write query here
    }
    data = fetch_filtered_data(query_parameters, name)
    
    prefix = f'{name} - '
    query_hash = hashlib.md5((json.dumps(query_parameters)).encode('utf-8')).hexdigest()
    file_name = prefix + query_hash
    with open(f'.static/responses/{file_name}.json', 'w') as writer:
        writer.write(json.dumps(data))
