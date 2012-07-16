import config
from common import flatten, require, YtError, parse_bool, add_quotes, union, unlist
from http import make_request
from table import get_yson_name, to_table
from tree_commands import exists, remove, get_attribute, set
from file_commands import upload_file
from operation_commands import \
        get_operation_stderr, get_operation_result, get_jobs_errors


import os
import types
import random
import string
import simplejson as json
from itertools import imap, ifilter

class Buffer(object):
    """ Reads line iterator by chunks """
    def __init__(self, lines_iterator, has_eoln=True):
        self._lines_iterator = lines_iterator
        self._empty = False
        self._has_eoln = has_eoln

    def get(self, bytes=None):
        if bytes is None: bytes = config.WRITE_BUFFER_SIZE
        sep = "" if self._has_eoln else "\n"
        if isinstance(self._lines_iterator, types.ListType):
            self._empty = True
            return sep.join(self._lines_iterator)
        read_bytes = 0
        result = []
        while read_bytes < bytes:
            try:
                line = self._lines_iterator.next()
            except StopIteration:
                self._empty = True
                return sep.join(result)
            read_bytes += len(line)
            result.append(line)
        return sep.join(result)

    def empty(self):
        return self._empty

""" Common table methods """
def create_table(path, make_it_empty=True):
    create = True
    if exists(path):
        require(get_attribute(path, "type") == "table",
                YtError("You try to create table by existing path "
                        "whose type differs from table"))
        if make_it_empty:
            remove(path)
            create = True
        else:
            create = False
    if create:
        dirname = os.path.dirname(path)
        if not exists(dirname):
            set(dirname, "{}")
        make_request("POST", "create", {"path": path, "type": "table"})

def create_temp_table(path, prefix=None):
    require(exists(path), YtError("You cannot create table in unexisting path"))
    # TODO(ignat): move it to configs?
    LENGTH = 10
    char_set = string.ascii_lowercase + string.ascii_uppercase + string.digits
    while True:
        name = "%s/%s%s" % (path, prefix, "".join(random.sample(char_set, LENGTH)))
        if not exists(name, hint=path):
            create_table(name)
            return name

def write_table(table, lines, format=None):
    if format is None: format = config.DEFAULT_FORMAT
    table = to_table(table)
    create_table(table.name, not table.append)
    buffer = Buffer(lines)
    while not buffer.empty():
        make_request("PUT", "write", {"path": table.name}, buffer.get(), format=format)

def read_table(table, format=None):
    def add_eoln(str):
        return str + "\n"
    if format is None: format = config.DEFAULT_FORMAT
    table = to_table(table)
    if not exists(table.name):
        return []
    response = make_request("GET", "read",
                            {"path": get_yson_name(table)}, format=format,
                            raw_response=True)
    return imap(add_eoln, ifilter(bool, response.iter_lines()))

def remove_table(table):
    if exists(table) and get_attribute(table, "type") == "table":
        remove(table)

def copy_table(source_table, destination_table, strategy=None):
    mode = "sorted" if all(map(is_sorted, flatten(source_table))) else "ordered"
    merge_tables(source_table, destination_table, mode)

def move_table(source_table, destination_table):
    copy_table(source_table, destination_table)
    remove(source_table)

def records_count(table):
    require(exists(table), YtError("Table %s doesn't exist" % table))
    return get_attribute(table, "row_count")

def is_sorted(table):
    require(exists(table), YtError("Table %s doesn't exist" % table))
    return parse_bool(get_attribute(table, "sorted"))

def sort_table(table, destination_table=None, columns=None, strategy=None, spec=None):
    # TODO(ignat): support list of input tables
    if strategy is None: strategy = config.DEFAULT_STRATEGY
    if columns is None: columns= ["key", "subkey"]
    if spec is None: spec = {}

    table = flatten(table)
    if destination_table is None:
        require(len(table) == 1,
                YtError("You must specify destination sort table "
                        "in case of multiple source tables"))
        destination_table = to_table(table[0])
    else:
        destination_table = to_table(destination_table)

    in_place = destination_table.name == unlist(table)
    if in_place:
        table = table[0]
        output_table = create_temp_table(os.path.dirname(table),
                                         os.path.basename(table))
    else:
        output_table = destination_table.name
        create_table(output_table, not destination_table.append)
    params = json.dumps(
        {"spec": union(spec,
            {"input_table_paths": flatten(table),
             "output_table_path": output_table,
             "key_columns": columns})})
    operation = add_quotes(make_request("POST", "sort", None, params))
    strategy.process_operation("sort", operation)
    if in_place:
        move_table(output_table, table)

def merge_tables(source_table, destination_table, mode, strategy=None, spec=None):
    if strategy is None: strategy = config.DEFAULT_STRATEGY
    if spec is None: spec = {}
    source_table = flatten(source_table)
    destination_table = to_table(destination_table)
    require(destination_table.name not in source_table,
            YtError("Destination should differ from source tables in merge operation"))
    create_table(destination_table.name,
                 make_it_empty=not destination_table.append)

    params = json.dumps(
        {"spec": union(spec,
            {"input_table_paths": source_table,
             "output_table_path": destination_table.name,
             "mode": mode})})
    operation = add_quotes(make_request("POST", "merge", None, params))
    strategy.process_operation("merge", operation)


""" Map and reduce methods """
def run_operation(binary, source_table, destination_table,
                  files, format, strategy, spec, op_type,
                  columns=None, replace_files=True):
    if strategy is None: strategy = config.DEFAULT_STRATEGY
    if format is None: format = config.DEFAULT_FORMAT
    if columns is None: columns = "key"
    if files is None: files = []
    if spec is None: spec = {}
    files = flatten(files)

    file_paths = []
    for file in files:
       file_paths.append(upload_file(file, replace=True))

    source_table = map(to_table, flatten(source_table))
    if config.MERGE_SRC_TABLES_BEFORE_OPERATION and len(source_table) > 1:
        temp_table = create_temp_table(config.TEMP_TABLES_STORAGE, "map_operation")
        merge_tables(source_table, temp_table, "ordered")
        source_table = [temp_table]
    for table in source_table:
        if not exists(table.name):
            create_table(table.name)
        if op_type == "reduce" and config.FORCE_SORT_IN_REDUCE and not is_sorted(table.name):
            sort_table(table.name)
    destination_table = map(to_table, flatten(destination_table))
    for table in destination_table:
        create_table(table.name, not table.append)

    op_key = {
        "map": "mapper",
        "reduce": "reducer"}

    operation_descr = \
                {"command": binary,
                 "format": format.to_json(),
                 "file_paths": file_paths}
    if op_type == "reducer":
        operation_descr.update({"key_columns": columns})

    params = json.dumps(
        {"spec": union(spec,
            {"input_table_paths": map(get_yson_name, source_table),
             "output_table_paths": map(get_yson_name, destination_table),
             op_key[op_type]: operation_descr})})
    operation = add_quotes(make_request("POST", op_type, None, params))
    strategy.process_operation(op_type, operation)

def run_map(binary, source_table, destination_table,
            files=None, format=None, strategy=None, spec=None):
    run_operation(binary, source_table, destination_table,
                  files=files, format=format,
                  strategy=strategy, spec=spec,
                  op_type="map")

def run_reduce(binary, source_table, destination_table,
               files=None, format=None, strategy=None, columns=None, spec=None):
    run_operation(binary, source_table, destination_table,
                  files=files, format=format,
                  strategy=strategy, spec=spec,
                  columns=columns,
                  op_type="reduce")

