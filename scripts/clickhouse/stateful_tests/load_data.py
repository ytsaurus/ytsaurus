#!/usr/bin/python2.7
import argparse
import datetime
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

    return (index, name, type)


def parse_clickhouse_schema(schema):
    parse_result = []

    char_index = 0
    while char_index < len(schema):
        char_index, name, type = parse_clickhouse_schema_field(schema, char_index)
        if type.startswith('Nested('):
            nested_schema = parse_clickhouse_schema(type[len('Nested(') : -1])
            for nested_name, nested_type in nested_schema:
                parse_result.append((name + '.' + nested_name, nested_type))
        else:
            parse_result.append((name, type))
    return parse_result


def create_yt_schema(clickhouse_schema):
    yt_schema = []
    supported_column_indexes = []
    column_index = 0
    for name, clickhouse_type in clickhouse_schema:
        if ('.' not in name) and (clickhouse_type in clickhouse_to_yt_type):
            yt_schema.append({
                'name': name,
                'type':clickhouse_to_yt_type[clickhouse_type],
                'required': 'true'})
            supported_column_indexes.append(column_index)
        else:
            print 'drop column:', name, 'with type:', clickhouse_type
        column_index += 1
    return (yt_schema, supported_column_indexes)


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
        return 0
    else:
        return (datetime.datetime.strptime(clickhouse_date, '%Y-%m-%d') - datetime.datetime(1970,1,1)).days


def clickhouse_to_yt_datetime(clickhouse_datetime):
    if clickhouse_datetime == "0000-00-00 00:00:00":
        return 0
    else:
        return int((datetime.datetime.strptime(clickhouse_datetime, '%Y-%m-%d %H:%M:%S') - datetime.datetime(1970,1,1)).total_seconds())


def table_data_generator(input_path, schema, supported_column_indexes):
    input_table_file = open(input_path)
    line = input_table_file.readline().decode('ascii', errors='ignore')
    while line:
        if len(line) == 0:
            break
        row = parse_csv_row(line)
        row_data = {}
        schema_index = 0
        for index in supported_column_indexes:
            name = schema[schema_index]['name']
            type = schema[schema_index]['type']
            value = row[index]
            row_data[name] = value
            if type == 'date':
                row_data[name] = clickhouse_to_yt_date(value)
            elif type == 'datetime':
                row_data[name] = clickhouse_to_yt_datetime(value)
            elif type != 'string':
                try:
                    row_data[name] = int(value)
                except ValueError:
                    pass
            schema_index += 1
        line = input_table_file.readline().decode('ascii', errors='ignore')
        yield row_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--schema', help='Schema path', required=True)
    parser.add_argument('--output', help='Cypress output table path', required=True)
    parser.add_argument('--creation_attributes', help='YT table creation attributes', required=False, default="{}")
    parser.add_argument('input', help='Table with tskv data')

    args=parser.parse_args()

    clickhouse_schema = parse_clickhouse_schema(open(args.schema).read().replace('\n', ''))

    yt_schema, supported_column_indexes = create_yt_schema(clickhouse_schema)

    creation_attributes = yson.loads(args.creation_attributes)
    creation_attributes['schema'] = yt_schema
    yt_wrapper.create('table', args.output, attributes=creation_attributes)

    yt_wrapper.write_table(args.output, table_data_generator(args.input, yt_schema, supported_column_indexes))
