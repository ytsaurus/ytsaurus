import copy

_well_known_attributes = ["name", "type", "required", "sort_order", "aggregate", "expression", "lock", "group"]
_well_known_attributes_v3 = [a for a in _well_known_attributes if a not in ["type", "required"]] + ["type_v3"]


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


def normalize_schema_v3(schema, attributes=_well_known_attributes_v3):
    return extract_column_attributes(schema, attributes)


# Better named alias
normalize_schema = extract_column_attributes
