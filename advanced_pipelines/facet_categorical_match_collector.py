from advanced_pipelines.utils import (
    read_excel_data_as_dict, fetch_data_from_collection,
    match_query_builder)
import string


def facet_query_builder(filters):
    facet_stage = {}
    
    # do not include items having these values in id
    ignore_values = [None, '', '0', 0, 0.0,] + list(string.whitespace)
    
    # build a facet stage for each select filters,
    # each facet item will have 3 stages:
    # 1. match - get rid of unwanted values having id in ignore_values
    # 2. group - group to get unique elements of each select filters
    # 3. project - remove _id field from group stage
    for key, value in filters.items():
        # if filter item is not select type, ignore it
        if not value['input']:
            continue
        if value['type'] != 'select':
            continue
        
        # get the uppercase of item_id -> id and item_title -> title
        # for all items in this filter, uppercased item_id and item_title
        # maps to required field names in the database
        item_id = value['item_id'].upper()
        item_title = value['item_title'].upper()
        
        # match stage will forward items whose item_id or item_title
        # is not one of the values in ignore_values list
        item_match = {
            item_id: {'$nin': ignore_values},
            item_title: {'$nin': ignore_values}}
        
        # group stage
        item_group = {}
        # if item_id and item_title are same field, group that field only
        # otherwise group the data using both
        if item_id == item_title:
            item_group['_id'] = {item_id: f'${item_title}'}
        else:
            item_group['_id'] = {
                item_id: f'${item_id}',
                item_title: f'${item_title}'}
        
        # get the item_id field -> id and item_title field -> title
        item_group['id'] = {'$first': f'${item_id}'}
        item_group['title'] = {'$first': f'${item_title}'}
        
        if 'special' in value:
            if value['special']:
                item_group['type'] = {'$first': '$TYPE'}
        # assign selected -> True (for frontend use)
        item_group['selected'] = {'$first': True}
        item_group['visible'] = {'$first': True}
        
        # project stage - get rid of _id
        item_project = {'_id': 0}
        item_sort = {'title': 1}
        
        # build the facet item query: match -> group -> project
        facet_item = [
            {'$match': item_match},
            {'$group': item_group},
            {'$project': item_project},
            {'$sort': item_sort}
        ]
        # assign the facet item to facet stage
        facet_stage[key] = facet_item
    
    return facet_stage


def process_filters(data, filters, query_parameters):
    filter_data = []
    for key, value in filters.items():
        # if input parameter is False, don't do anything
        if not value['input']:
            continue
        # if type parameter is radio, don't do anything
        if value['type'] == 'radio':
            continue
        
        item_data = value
        # remove keys not necessary in frontend
        del item_data['item_id']
        del item_data['item_title']
        del item_data['output']
        del item_data['input']
        
        if value['type'] == 'select':
            # remove min-max keys for categorical fields
            del item_data['min']
            del item_data['max']
            # add items retrieved from database with filter option
            item_data['items'] = data[key]
        elif value['type'] in {'date', 'range'}:
            # if filter type is data or range type,
            # if there were query parameters, add them,
            # otherwise set to default, i.e. empty string
            if key in query_parameters:
                item_data['max'] = query_parameters[key][0]
                item_data['min'] = query_parameters[key][1]
            else:
                item_data['max'] = ''
                item_data['min'] = ''
        
        # add filter data to processed filter
        filter_data.append(item_data)
    
    return filter_data


def fetch_matched_updates(query_parameters, name):
    # read excel sheet to make response structure,
    # build match query, and classify filters
    filters = read_excel_data_as_dict(name)
    match_query = match_query_builder(query_parameters, filters)
    
    # build facet query to collect matching categorical items
    facet_query = facet_query_builder(filters)
    
    collection_name = f'{name}_statistics_collection'
    pipeline = [
        {'$match': match_query},
        {'$facet': facet_query},]
    
    # data fetched from MongoDB collection as list of dicts
    data = fetch_data_from_collection(collection_name, pipeline)[0]
    # integrate data from database with processed data from s3 excel sheet
    filter_data = process_filters(data, filters, query_parameters)
    
    return filter_data
