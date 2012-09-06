import config
import py_wrapper
from common import flatten, require, YtError, parse_bool, unlist, update
from path_tools import escape_path
from http import make_request
from table import get_yson_name, to_table, to_name
from tree_commands import exists, remove, get_attribute, copy, mkdir, find_free_subpath
from file_commands import upload_file

import os
import types
import simplejson as json
from itertools import imap, ifilter

""" Auxiliary methods """
def prepare_source_tables(tables):
    return map(to_table, filter(exists, map(to_name, flatten(tables))))

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
        mkdir(dirname)
        make_request("POST", "create",
                     {"path": escape_path(path),
                      "type": "table"})

def create_temp_table(path=None, prefix=None):
    if path is None: path = config.TEMP_TABLES_STORAGE
    require(exists(path), YtError("You cannot create table in unexisting path"))
    if prefix is not None:
        path = os.path.join(path, prefix)
    else:
        if not path.endswith("/"):
            path = path + "/"
    name = find_free_subpath(path)
    create_table(name)
    return name

def write_table(table, lines, format=None, codec_id=None):
    if format is None: format = config.DEFAULT_FORMAT
    table = to_table(table)
    create_table(table.name, not table.append)

    params = {"path": table.escaped_name()}
    if codec_id is not None:
        params["table_writer"] = {"codec_id": codec_id}

    buffer = Buffer(lines)
    while not buffer.empty():
        make_request("PUT", "write", params, buffer.get(), format=format)

def read_table(table, format=None):
    def add_eoln(str):
        return str + "\n"
    if format is None: format = config.DEFAULT_FORMAT
    table = to_table(table)
    print "table", table.name
    if not exists(table.name):
        return []
    response = make_request("GET", "read",
                            {"path": get_yson_name(table)}, format=format,
                            raw_response=True, verbose=True)
    return response.iter_lines(chunk_size=config.READ_BUFFER_SIZE)
    return imap(add_eoln, ifilter(bool, response.iter_lines(chunk_size=config.READ_BUFFER_SIZE)))

def remove_table(table):
    if exists(table) and get_attribute(table, "type") == "table":
        remove(table)

def copy_table(source_table, destination_table, strategy=None):
    source_tables = prepare_source_tables(source_table)
    destination_table = to_table(destination_table)
    require(len(source_tables) > 0,
            YtError("You try to copy unexisting tables"))
    #require(not destination_table.has_delimiters(),
    #        YtError("You cannot make copy to table with delimiters"))
    destination_table = to_table(destination_table)
    if len(source_tables) == 1 and not destination_table.append:
        if exists(destination_table.name):
            remove(destination_table.name)
        copy(source_tables[0].name, destination_table.name)
    else:
        source_names = [table.name for table in source_tables]
        mode = "sorted" if all(map(is_sorted, source_names)) else "ordered"
        merge_tables(source_tables, destination_table, mode, strategy=strategy)

def move_table(source_table, destination_table):
    copy_table(source_table, destination_table)
    remove_table(source_table)

def records_count(table):
    require(exists(table), YtError("Table %s doesn't exist" % table))
    return get_attribute(table, "row_count")

def is_empty(table):
    return records_count(table) == 0

def is_sorted(table):
    require(exists(table), YtError("Table %s doesn't exist" % table))
    return parse_bool(get_attribute(table, "sorted"))

def sort_table(source_table, destination_table=None, sort_by=None, strategy=None, spec=None):
    if strategy is None: strategy = config.DEFAULT_STRATEGY
    if spec is None: spec = {}
    if sort_by is None:
        require(hasattr(config.DEFAULT_FORMAT, "has_subkey"),
                YtError("You must pass sort_by parameter to sort operation"))
        sort_by = ["key", "subkey"]
        #if config.DEFAULT_FORMAT.has_subkey:
        #    sort_by.append("subkey")

    source_table = map(to_table, flatten(source_table))
    source_table = filter(lambda table: exists(table.name), source_table)
    if not source_table or get_attribute(source_table, "sorted", default=[]) == sort_by:
        return

    if destination_table is None:
        require(len(source_table) == 1 and not source_table[0].has_delimiters(),
                YtError("You must specify destination sort table "
                        "in case of multiple source tables"))
        destination_table = to_table(source_table[0])
    else:
        destination_table = to_table(destination_table)

    in_place = destination_table == unlist(source_table)
    if in_place:
        source_table = source_table[0]
        output_table = create_temp_table(os.path.dirname(source_table.name),
                                         os.path.basename(source_table.name))
    else:
        output_table = destination_table.name
        create_table(output_table, not destination_table.append)
    params = json.dumps(
        {"spec": update(spec,
            {"input_table_paths": map(get_yson_name, flatten(source_table)),
             "output_table_path": escape_path(output_table),
             "sort_by": sort_by})})
    operation = make_request("POST", "sort", None, params)
    strategy.process_operation("sort", operation)
    if in_place:
        move_table(output_table, source_table)

def merge_tables(source_table, destination_table, mode, strategy=None, spec=None):
    if strategy is None: strategy = config.DEFAULT_STRATEGY
    if spec is None: spec = {}
    source_table = map(to_table, flatten(source_table))
    destination_table = to_table(destination_table)
    require(destination_table.name not in map(lambda x: x.name, source_table),
            YtError("Destination should differ from source tables in merge operation"))
    create_table(destination_table.name,
                 make_it_empty=not destination_table.append)

    params = json.dumps(
        {"spec": update(spec,
            {"input_table_paths": map(get_yson_name, source_table),
             "output_table_path": destination_table.escaped_name(),
             "mode": mode})})
    operation = make_request("POST", "merge", None, params)
    strategy.process_operation("merge", operation)



""" Map and reduce methods """
def _prepare_files(files):
    if files is None:
        return []

    file_paths = []
    for file in flatten(files):
        file_paths.append(upload_file(file))
    return file_paths

class Finalizer(object):
    def __init__(self, files, output_tables):
        self.files = files if files is not None else []
        self.output_tables = output_tables
    
    def __call__(self):
        for table in filter(is_empty, map(to_name, self.output_tables)):
            remove_table(table)
        for file in self.files:
            remove(file)

def _add_output_fd_redirect(binary, dst_count):
    if config.USE_MAPREDUCE_STYLE_DST_TABLES:
        for fd in xrange(3, 3 + dst_count):
            yt_fd = 1 + (fd - 3) * 3
            binary = binary + " %d>&%d" % (fd, yt_fd)
    return binary

def _prepare_formats(format, input_format, output_format):
    if format is None: format = config.DEFAULT_FORMAT
    if input_format is None: input_format = format
    if output_format is None: output_format = format
    return input_format, output_format

def run_operation(binary, source_table, destination_table,
                  files, file_paths, 
                  format, input_format, output_format,
                  strategy, spec, op_type,
                  reduce_by=None):
    if strategy is None: strategy = config.DEFAULT_STRATEGY
    if reduce_by is None: reduce_by = "key"
    if spec is None: spec = {}
    input_format, output_format = _prepare_formats(format, input_format, output_format)

    files = _prepare_files(files)
    if isinstance(binary, types.FunctionType):
        binary, additional_files = py_wrapper.wrap(binary)
        files += _prepare_files(additional_files)
    if file_paths is None:
        file_paths = []
    file_paths += files

    source_table = map(to_table, flatten(source_table))
    if config.MERGE_SRC_TABLES_BEFORE_OPERATION and len(source_table) > 1:
        temp_table = create_temp_table(config.TEMP_TABLES_STORAGE, "map_operation")
        merge_tables(source_table, temp_table, "ordered")
        source_table = [temp_table]
    source_table = filter(lambda table: exists(table.name), source_table)
    for table in source_table:
        if op_type == "reduce" and config.FORCE_SORT_IN_REDUCE and not is_sorted(table.name):
            sort_table(table.name)
    destination_table = map(to_table, flatten(destination_table))
    for table in destination_table:
        create_table(table.name, not table.append)

    binary = _add_output_fd_redirect(binary, len(destination_table))

    op_key = {
        "map": "mapper",
        "reduce": "reducer"}

    operation_descr = \
                {"command": binary,
                 "file_paths": map(escape_path, file_paths),
                 "input_format": input_format.to_json(),
                 "output_format": output_format.to_json()}
    if op_type == "reduce":
        operation_descr.update({"reduce_by": reduce_by})

    params = json.dumps(
        {"spec": update(spec,
            {"input_table_paths": map(get_yson_name, source_table),
             "output_table_paths": map(get_yson_name, destination_table),
             op_key[op_type]: operation_descr})})
    operation = make_request("POST", op_type, None, params)
    strategy.process_operation(op_type, operation, Finalizer(files, destination_table))

def run_map(binary, source_table, destination_table,
            files=None, file_paths=None,
            format=None, input_format=None, output_format=None,
            strategy=None, spec=None):
    run_operation(binary, source_table, destination_table,
                  files=files, file_paths=file_paths,
                  format=format,
                  input_format=input_format,
                  output_format=output_format,
                  strategy=strategy, spec=spec,
                  op_type="map")

def run_reduce(binary, source_table, destination_table,
               files=None, file_paths=None, 
               format=None, input_format=None, output_format=None,
               strategy=None, reduce_by=None, spec=None):
    run_operation(binary, source_table, destination_table,
                  files=files, file_paths=file_paths,
                  format=format,
                  input_format=input_format,
                  output_format=output_format,
                  strategy=strategy, spec=spec,
                  reduce_by=reduce_by,
                  op_type="reduce")

def run_map_reduce(mapper, reducer, source_table, destination_table,
                   format=None, input_format=None, output_format=None,
                   strategy=None, spec=None,
                   map_files=None, reduce_files=None,
                   map_file_paths=None, reduce_file_paths=None,
                   sort_by=None, reduce_by=None):
    if strategy is None: strategy = config.DEFAULT_STRATEGY
    if reduce_by is None and sort_by is None:
        sort_by = ["key", "subkey"]
        reduce_by = ["key"]
    
    input_format, output_format = _prepare_formats(format, input_format, output_format)
    
    run_map_reduce.spec = {} if spec is None else spec
    run_map_reduce.files_to_remove = []
    def prepare_operation(binary, files, file_paths, spec_keyword):
        if file_paths is None: file_paths = []
        """ Returns new spec """
        files = _prepare_files(files)
        if binary is not None:
            if isinstance(binary, types.FunctionType):
                binary, additional_files = py_wrapper.wrap(binary)
                files += _prepare_files(additional_files)
            run_map_reduce.spec = update(run_map_reduce.spec, 
                {
                    spec_keyword: {
                        "input_format": input_format.to_json(),
                        "output_format": output_format.to_json(),
                        "command": binary,
                        "file_paths": flatten(files + file_paths)
                    }
                })
        run_map_reduce.files_to_remove += files
    
    prepare_operation(mapper, map_files, map_file_paths, "mapper")
    prepare_operation(reducer, reduce_files, reduce_file_paths, "reducer")

    source_table = prepare_source_tables(source_table)
    destination_table = map(to_table, flatten(destination_table))
    for table in destination_table:
        create_table(table.name, not table.append)

    reducer = _add_output_fd_redirect(reducer, len(destination_table))

    params = json.dumps(
        {"spec": update(run_map_reduce.spec,
            {"sort_by": flatten(sort_by),
             "reduce_by": flatten(reduce_by),
             "input_table_paths": map(get_yson_name, source_table),
             "output_table_paths": map(get_yson_name, destination_table)
            })
        })
    operation = make_request("POST", "map_reduce", None, params)
    strategy.process_operation("map_reduce", operation,
         Finalizer(run_map_reduce.files_to_remove, destination_table))
    
