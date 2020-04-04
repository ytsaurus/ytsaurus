#!/usr/bin/python2.7
import argparse
import datetime
import dateutil.tz
import time
import os
import yt.wrapper as yt_wrapper
import yt.yson as yson
from tqdm import tqdm


clickhouse_to_yt_type = {
    'Date' : 'date',
    'DateTime' : 'datetime',
    'Int16' : 'int16',
    'Int32' : 'int32',
    'Int64' : 'int64',
    'Int8' : 'int8',
    'String' : 'string',
    'UInt16' : 'uint16',
    'UInt32' : 'uint32',
    'UInt64' : 'uint64',
    'UInt8' : 'uint8',
    'Float64' : 'double',
}

default_yt_type_value = {
    'date': yson.YsonUint64(0),
    'datetime': yson.YsonUint64(0),
    'int16': yson.YsonInt64(0),
    'int32': yson.YsonInt64(0),
    'int64': yson.YsonInt64(0),
    'int8': yson.YsonInt64(0),
    'string': "",
    'uint16': yson.YsonUint64(0),
    'uint32': yson.YsonUint64(0),
    'uint64': yson.YsonUint64(0),
    'uint8': yson.YsonUint64(0),
    'double': yson.YsonDouble(0.0),
}

def parse_clickhouse_schema_field(schema, index):
    while index < len(schema) and schema[index].isspace():
        index += 1
    name_start_index = index
    while index < len(schema) and not schema[index].isspace():
        index += 1

    name = schema[name_start_index:index]

    while index < len(schema) and schema[index].isspace():
        index += 1
    bracket_balance = 0
    type_start_index = index
    while index < len(schema) and (schema[index] != ',' or bracket_balance != 0):
        if schema[index] == '(':
            bracket_balance += 1
        elif schema[index] == ')':
            bracket_balance -= 1
        index += 1

    type = schema[type_start_index:index]
    index += 1

    return (index, [name], type)


def parse_clickhouse_schema(schema):
    parse_result = []
    char_index = 0
    while char_index < len(schema):
        char_index, name, type = parse_clickhouse_schema_field(schema, char_index)
        if type.startswith('Nested'):
            nested_schema = parse_clickhouse_schema(type[len('Nested(') : -1])
            for nested_name, nested_type in nested_schema:
                parse_result.append((name + nested_name, nested_type))
        elif type.startswith('FixedString'):
            parse_result.append((name, 'String'))
        else:
            parse_result.append((name, type))
    return parse_result


def is_field_simple(name_path, clickhouse_type):
    return len(name_path) == 1 and clickhouse_type in clickhouse_to_yt_type

def is_field_list(name_path, clickhouse_type):
    return clickhouse_type.startswith('Array')

def is_field_struct(name_path, clickhouse_type):
    return len(name_path) > 1

def create_simple_yt_schema_field(name, clickhouse_type):
    return {'name': name, 'type': clickhouse_to_yt_type[clickhouse_type], 'required': 'true'};

def create_list_yt_schema_field(name, clickhouse_type):
    return {'name': name, 'type_v3': {'type_name': 'list', 'item': clickhouse_to_yt_type[clickhouse_type[len('Array('):-1]]}, 'required': 'true'};

def create_struct_yt_schema_field(clickhouse_schema, column_index):
    name_path, clickhouse_type = clickhouse_schema[column_index]
    struct_schema_field = {'name': name_path[0], 'type_v3': {'type_name': 'struct', 'members':[]}}
    while column_index < len(clickhouse_schema) and name_path[0] == struct_schema_field['name']:
        struct_schema_field['type_v3']['members'].append(create_simple_yt_schema_field(name_path[1], clickhouse_type))
        column_index += 1
        if column_index < len(clickhouse_schema):
            name_path, clickhouse_type = clickhouse_schema[column_index]
    return struct_schema_field, column_index - 1


def create_yt_schema(clickhouse_schema):
    yt_schema = []
    column_index = 0
    while column_index < len(clickhouse_schema):
        name_path, clickhouse_type = clickhouse_schema[column_index]
        if is_field_simple(name_path, clickhouse_type):
            yt_schema.append(create_simple_yt_schema_field(name_path[0], clickhouse_type))
        elif is_field_list(name_path, clickhouse_type):
            yt_schema.append(create_list_yt_schema_field(name_path[0], clickhouse_type))
        elif len(name_path) > 1:
            struct_schema_field, column_index = create_struct_yt_schema_field(clickhouse_schema, column_index)
            yt_schema.append(struct_schema_field)
        else:
            raise Exception("cannot parse clickhouse schema field: name_path:", name_path, "type:", clickhouse_type)
        column_index += 1

    return yt_schema


def parse_csv_row(line):
    row = []
    index = 0
    while index < len(line):
        item = ''
        while index < len(line) and line[index] != '\t':
            item = item + line[index]
            index += 1
        row.append(item)
        index += 1
    return row


def clickhouse_to_yt_date(clickhouse_date):
    if clickhouse_date == "0000-00-00":
        return yson.YsonUint64(0)
    else:
        return yson.YsonUint64((datetime.datetime.strptime(clickhouse_date, '%Y-%m-%d') - datetime.datetime(1970,1,1)).days)


def clickhouse_to_yt_datetime(clickhouse_datetime):
    if clickhouse_datetime == "0000-00-00 00:00:00":
        return yson.YsonUint64(0)
    else:
        result = yson.YsonUint64((datetime.datetime.strptime(clickhouse_datetime, '%Y-%m-%d %H:%M:%S') - datetime.datetime(1970,1,1, hour=4)).total_seconds())
        if result < 0:
            print "negative date after timezone shift:", result
            return yson.YsonUint64(0)
        return result

def cast_value_to_yt_type(value, item_schema):
    if 'type' in item_schema:
        type = item_schema['type']
        if type == 'string':
            return value
        elif value.startswith('['):
            return default_yt_type_value[type]
        elif type == 'date':
            return clickhouse_to_yt_date(value)
        elif type == 'datetime':
            return clickhouse_to_yt_datetime(value)
        elif type.startswith("int"):
            return yson.YsonInt64(value)
        elif type.startswith("uint"):
            return yson.YsonUint64(value)
        elif type.startswith("double"):
            return yson.YsonDouble(float(value))
    else:
        if item_schema['type_v3']['type_name'] == 'list':
            result = []
            if value != '[]':
                for splitted_value in value[1:-1].split(','):
                    result.append(cast_value_to_yt_type(splitted_value, {'type': item_schema['type_v3']['item']}))
            return result

def table_data_generator(input_path, name_paths, yt_schema):
    input_table_file = open(input_path)
    line = input_table_file.readline().decode('utf-8', errors='replace')
    while line:
        if len(line) == 0:
            break
        row_values = parse_csv_row(line)
        assert len(row_values) == len(clickhouse_schema)

        row_data = {}
        for column_index in range(len(row_values)):
            def fill_row_data_item(row_data_item, name_path, value, yt_schema):
                name = name_path[0]
                item_schema = {}
                for schema in yt_schema:
                    if schema['name'] == name:
                        item_schema = schema
                if len(name_path) > 1:
                    if name not in row_data_item:
                        row_data_item[name] = {}
                    yt_subschema = {}
                    fill_row_data_item(row_data_item[name], name_path[1:], value, item_schema['type_v3']['members'])
                else:
                    if 'type' in item_schema:
                        yt_type = item_schema['type']
                    else:
                        yt_type = item_schema['type_v3']['item']
                    row_data_item[name] = cast_value_to_yt_type(value, item_schema)
            fill_row_data_item(row_data, name_paths[column_index], row_values[column_index], yt_schema)
            column_index += 1

        line = input_table_file.readline().decode('utf-8', errors='replace')
        yield row_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--schema', help='Schema path', required=True)
    parser.add_argument('--output', help='Cypress output table path', required=True)
    parser.add_argument('--creation_attributes', help='YT table creation attributes', required=False, default="{}")
    parser.add_argument('input', help='Table with tskv data')

    args=parser.parse_args()

    clickhouse_schema = parse_clickhouse_schema(open(args.schema).read().replace('\n', ''))
    yt_schema = create_yt_schema(clickhouse_schema)

    creation_attributes = yson.loads(args.creation_attributes)
    creation_attributes['schema'] = yt_schema
    creation_attributes['optimize_for'] = 'scan'
    yt_wrapper.create('table', args.output, attributes=creation_attributes)

    yt_wrapper.write_table(args.output, table_data_generator(args.input, [name_path for name_path, clickhuose_type in clickhouse_schema], yt_schema))
