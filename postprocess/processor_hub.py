from postprocess.datatype_mapper import (
    build_datatype_mapper, build_set_query, update_datatype_inplace
)


def update_collection_field_datatypes(name, sheet_name, true_values_only):
    mapper = build_datatype_mapper(name, sheet_name, true_values_only)
    set_query = build_set_query(mapper)
    update_datatype_inplace(name, set_query)
