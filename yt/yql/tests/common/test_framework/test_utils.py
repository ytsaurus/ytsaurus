import six


def row_spec_to_yt_schema(row_spec):
    import yt.yson

    def toYtType(yqlType):
        while yqlType[0] == "TaggedType":
            yqlType = yqlType[2]

        required = True
        if yqlType[0] == "OptionalType":
            yqlType = yqlType[1]
            required = False

        if yqlType[0] != "DataType":
            return {"type": "any", "required": False}

        yqlType = yqlType[1]
        if yqlType in set(
            ["String", "Json", "JsonDocument", "Longint", "Uuid", "Decimal",
                "TzDate", "TzDatetime", "TzTimestamp", "TzDate32", "TzDatetime64", "TzTimestamp64", "DyNumber"]
                ):
            return {"type": "string", "required": required}
        elif yqlType == "Utf8":
            return {"type": "utf8", "required": required}
        elif yqlType == "Int64" or yqlType in ["Interval", "Datetime64", "Timestamp64", "Interval64"]:
            return {"type": "int64", "required": required}
        elif yqlType == "Int32" or yqlType == "Date32":
            return {"type": "int32", "required": required}
        elif yqlType == "Int16":
            return {"type": "int16", "required": required}
        elif yqlType == "Int8":
            return {"type": "int8", "required": required}
        elif yqlType == "Uint64" or yqlType == "Timestamp":
            return {"type": "uint64", "required": required}
        elif yqlType == "Uint32" or yqlType == "Datetime":
            return {"type": "uint32", "required": required}
        elif yqlType == "Uint16" or yqlType == "Date":
            return {"type": "uint16", "required": required}
        elif yqlType == "Uint8":
            return {"type": "uint8", "required": required}
        elif yqlType == "Double" or yqlType == "Float":
            return {"type": "double", "required": required}
        elif yqlType == "Bool":
            return {"type": "boolean", "required": required}
        elif yqlType == "Yson":
            return {"type": "any", "required": False}
        raise Exception("Unknown type %s" % yqlType)

    columns = {name: toYtType(yqlType) for name, yqlType in row_spec["Type"][1]}
    schema = yt.yson.YsonList()
    if 'SortedBy' in row_spec:
        for i in range(len(row_spec['SortedBy'])):
            column = row_spec['SortedBy'][i]
            sColumn = {'name': column, 'sort_order': 'ascending'}
            sColumn.update(toYtType(row_spec['SortedByTypes'][i]))
            schema.append(sColumn)
            columns.pop(column, None)
    for column in six.iterkeys(columns):
        sColumn = {'name': column}
        sColumn.update(columns[column])
        schema.append(sColumn)
    schema.attributes["strict"] = row_spec.get("StrictSchema", True)
    return schema


def infer_yt_schema(attrs):
    import yt.yson

    attrs = yt.yson.loads(attrs.encode())
    if 'schema' not in attrs and '_yql_row_spec' in attrs:
        attrs['schema'] = row_spec_to_yt_schema(attrs['_yql_row_spec'])

    return yt.yson.dumps(attrs, yson_format="pretty").decode()
