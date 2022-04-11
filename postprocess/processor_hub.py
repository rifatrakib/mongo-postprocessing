from postprocess.datatype_mapper import build_datatype_mapper, build_type_conversion_query
from postprocess.field_validator import build_validation_mapper, build_validation_query
from postprocess.utils import update_datatype_inplace


def update_collection_field_datatypes(name, sheet_name):
    datatype_mapper = build_datatype_mapper(name, sheet_name)
    set_query = build_type_conversion_query(datatype_mapper)
    update_datatype_inplace(name, set_query)


def validate_collection_field_values(name, sheet_name):
    validation_mapper = build_validation_mapper(name, sheet_name)
    set_query = build_validation_query(validation_mapper)
    update_datatype_inplace(name, set_query)
