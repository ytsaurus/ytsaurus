import copy

_well_known_attributes = ["name", "type", "required", "sort_order", "aggregate", "expression", "lock", "group"]

def extract_column_attributes(schema, attributes=_well_known_attributes):
    def delete_other_attributes(schema):
        for column in schema:
            keys = list(column.keys())
            for key in keys:
                if key not in attributes:
                    del column[key]
    schema_copy = copy.deepcopy(schema)
    if isinstance(schema, list):
        delete_other_attributes(schema_copy)
    elif isinstance(schema, dict) and "$value" in schema:
        delete_other_attributes(schema_copy["$value"])
    else:
        raise TypeError("Expected schema to be a list of columns or a dict with key \"$value\", got {}".format(schema))
    return schema_copy
