from postprocess.statistical_collection_builder import build_field_category_mapper, fetch_statistical_data
from postprocess.datatype_mapper import build_datatype_mapper, build_type_conversion_query
from postprocess.field_validator import build_validation_mapper, build_validation_query
from postprocess.utils import update_datatype_inplace, rebuild_statistical_collection
from postprocess.duplicate_handler import label_fields_setter


def update_collection_field_datatypes(name, sheet_name):
    datatype_mapper = build_datatype_mapper(name, sheet_name)
    set_query = build_type_conversion_query(datatype_mapper)
    update_datatype_inplace(name, set_query)


def validate_collection_field_values(name, sheet_name):
    validation_mapper = build_validation_mapper(name, sheet_name)
    set_query = build_validation_query(validation_mapper)
    update_datatype_inplace(name, set_query)


def resolve_collection_duplicate_data(name):
    label_mapper = {
        'city': {
            'field_name': 'city',
            'field_id': 'city_number',
            'parent_field': 'state',
        },
        'zipcode': {
            'field_name': 'zipcode',
            'field_id': 'area_code',
            'parent_field': 'city',
            'location': 'area'
        }
    }
    set_query = label_fields_setter('collection', label_mapper)
    update_datatype_inplace(name, set_query)


def statistical_collection_builder(name, sheet_name):
    field_category_mapper = build_field_category_mapper(name, sheet_name)
    data = fetch_statistical_data(name, field_category_mapper)
    rebuild_statistical_collection(name, data)
