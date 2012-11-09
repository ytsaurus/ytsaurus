import config
import py_wrapper
from common import flatten, require, YtError, unlist, update, EMPTY_GENERATOR, parse_bool, is_prefix
from http import make_request, read_content
from table import get_yson_name, get_output_yson_name, to_table, to_name
from tree_commands import exists, remove, remove_with_empty_dirs, get_attribute, copy, mkdir, find_free_subpath
from file_commands import smart_upload_file
from transaction_commands import add_transaction_params

import os
import types
import logger
from copy import deepcopy

""" Auxiliary methods """
def _filter_empty_tables(tables):
    filtered = []
    for table in tables:
        if not exists(table.name):
            logger.warning("Warning: input table '%s' does not exist", table.name)
        else:
            filtered.append(table)
    return filtered

def _prepare_source_tables(tables):
    return _filter_empty_tables(map(to_table, flatten(tables)))

def _prepare_reduce_by(reduce_by):
    if reduce_by is None:
        if config.MAPREDUCE_MODE:
            reduce_by = ["key"]
        else:
            raise YtError("reduce_by option is required")
    return flatten(reduce_by)

def _prepare_sort_by(sort_by):
    if sort_by is None:
        if config.MAPREDUCE_MODE:
            sort_by = ["key", "subkey"]
        else:
            raise YtError("sort_by option is required")
    return flatten(sort_by)

def _calc_job_count(input_tables, min_data=None, max_data=None):
    size = sum(map(get_size, input_tables))
    if min_data is None: min_data = config.MIN_SIZE_PER_JOB
    if max_data is None: max_data = config.MAX_SIZE_PER_JOB
    return max(1, min(size / min_data, max(config.CLUSTER_SIZE, size / max_data)))


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
def add_user_spec(params):
    result = deepcopy(params)
    result["spec"] = update(
        params.get("spec", {}),
        {"mr_user": os.environ.get("MR_USER", ""),
         "system_user": os.environ.get("USER", "")})
    return result

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
        make_request("create",
                     add_transaction_params(
                         {"path": path,
                          "type": "table"}))

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


def write_table(table, lines, format=None, table_writer=None):
    if format is None: format = config.DEFAULT_FORMAT
    table = to_table(table)
    if not exists(table.name):
        create_table(table.name)

    params = {}
    if table_writer is not None:
        params["table_writer"] = table_writer
    params = add_transaction_params(params)

    step = 0
    buffer = Buffer(lines)
    while not buffer.empty():
        if step == 0:
            params["path"] = table.yson_name("output")
        else:
            params["path"] = table.yson_name()
        make_request("write", params, buffer.get(), format=format)
        step += 1
    # TODO(ignat): what if lines is empty?

def read_table(table, format=None, response_type=None):
    if format is None: format = config.DEFAULT_FORMAT
    if response_type is None: response_type = "iter_lines"
    table = to_table(table)
    if not exists(table.name):
        return EMPTY_GENERATOR
    response = make_request("read",
                            add_transaction_params(
                                {"path": get_yson_name(table)}
                            ),
                            format=format,
                            raw_response=True)
    return read_content(response, response_type)

def remove_table(table):
    table = to_name(table)
    require(exists(table) and get_attribute(table, "type") == "table",
            "'%s' should exist and be table")
    remove(table)

def remove_table_with_empty_dirs(table):
    table = to_name(table)
    require(exists(table) and get_attribute(table, "type") == "table",
            "'%s' should exist and be table")
    remove_with_empty_dirs(table)

def copy_table(source_table, destination_table, strategy=None):
    source_tables = _prepare_source_tables(source_table)
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

def erase_table(table, strategy=None):
    if strategy is None: strategy = config.DEFAULT_STRATEGY
    table = to_table(table)
    if not exists(table.name):
        return
    params = add_transaction_params({
        "spec": {
            "table_path": table.yson_name()
        }
    })
    operation = make_request("erase", params)
    strategy.process_operation("erase", operation)

def records_count(table):
    require(exists(table), YtError("Table %s doesn't exist" % table))
    return get_attribute(to_name(table), "row_count")

def get_size(table):
    return get_attribute(to_name(table), "uncompressed_data_size")

def is_empty(table):
    return records_count(table) == 0

def get_sorted_by(table):
    return get_attribute(to_name(table), "sorted_by", default=[])

def is_sorted(table):
    require(exists(table), YtError("Table %s doesn't exist" % table))
    if config.MAPREDUCE_MODE:
        return get_sorted_by(table) == ["key", "subkey"]
    else:
        return parse_bool(get_attribute(to_name(table), "sorted", default="false"))


def merge_tables(source_table, destination_table, mode, strategy=None, table_writer=None, spec=None):
    source_table = _prepare_source_tables(source_table)
    destination_table = unlist(_prepare_destination_tables(destination_table))

    if spec is None: spec = {}
    if table_writer is not None:
        spec = update({"job_io": {"table_writer": table_writer}}, spec)

    params = \
        add_user_spec(add_transaction_params(
            {"spec": update(
                {"input_table_paths": map(get_yson_name, source_table),
                 "output_table_path": destination_table.yson_name("output"),
                 "mode": mode},
                spec)}))
    operation = make_request("merge", params)

    if strategy is None: strategy = config.DEFAULT_STRATEGY
    strategy.process_operation("merge", operation)


def sort_table(source_table, destination_table=None, sort_by=None, strategy=None, table_writer=None, spec=None):
    def prepare_job_count(spec, source_table):
        if "partition_count" not in spec:
            spec["partition_count"] = _calc_job_count(source_table, max_data=config.MAX_SIZE_PER_JOB/2)
            spec["min_data_size_per_sort_job"] = config.MIN_SIZE_PER_JOB

    sort_by = _prepare_sort_by(sort_by)
    source_table = _prepare_source_tables(source_table)
    if all(is_prefix(sort_by, get_sorted_by(table.name))
           for table in source_table):
        if len(source_table) > 0:
            merge_tables(source_table, destination_table, "sorted", strategy=strategy, table_writer=table_writer, spec=spec)
        return

    if destination_table is None:
        require(len(source_table) == 1 and not source_table[0].has_delimiters(),
                YtError("You must specify destination sort table "
                        "in case of multiple source tables"))
        destination_table = source_table[0]
    destination_table = unlist(_prepare_destination_tables(destination_table))

    if spec is None: spec = {}
    if table_writer is not None:
        spec = update({"sort_job_io": {"table_writer": table_writer}}, spec)
    spec = update(
                {"input_table_paths": map(get_yson_name, flatten(source_table)),
                 "output_table_path": destination_table.yson_name("output"),
                 "sort_by": sort_by},
                spec)
    prepare_job_count(spec, source_table)
    params = add_user_spec(add_transaction_params(
        {"spec": spec}
    ))
    operation = make_request("sort", params)

    if strategy is None: strategy = config.DEFAULT_STRATEGY
    strategy.process_operation("sort", operation)


""" Map and reduce methods """
def _prepare_files(files):
    if files is None:
        return []

    file_paths = []
    for file in flatten(files):
        file_paths.append(smart_upload_file(file))
    return file_paths

def _add_output_fd_redirect(binary, dst_count):
    if config.USE_MAPREDUCE_STYLE_DESTINATION_FDS:
        for fd in xrange(3, 3 + dst_count):
            yt_fd = 1 + (fd - 3) * 3
            binary = binary + " %d>/dev/fd/%d" % (fd, yt_fd)
    return binary

def _prepare_formats(format, input_format, output_format):
    if format is None: format = config.DEFAULT_FORMAT
    if input_format is None: input_format = format
    if output_format is None: output_format = format
    return input_format, output_format

def _prepare_binary(binary, files):
    if isinstance(binary, types.FunctionType):
        binary, additional_files = py_wrapper.wrap(binary)
        files += _prepare_files(additional_files)
    return binary

def _prepare_destination_tables(tables):
    tables = map(to_table, flatten(tables))
    for table in tables:
        if not exists(table.name):
            create_table(table.name)
    return tables


class Finalizer(object):
    def __init__(self, files, output_tables):
        self.files = files if files is not None else []
        self.output_tables = output_tables

    def __call__(self):
        for table in filter(is_empty, map(to_name, self.output_tables)):
            remove_table_with_empty_dirs(table)
        for file in self.files:
            remove_with_empty_dirs(file)


def run_map_reduce(mapper, reducer, source_table, destination_table,
                   format=None, input_format=None, output_format=None,
                   strategy=None, table_writer=None, spec=None,
                   map_files=None, reduce_files=None,
                   map_file_paths=None, reduce_file_paths=None,
                   sort_by=None, reduce_by=None):
    def prepare_job_count(spec, source_table):
        if "map_job_count" not in spec and "partition_count" not in spec:
            spec["map_job_count"] = _calc_job_count(source_table)
            spec["partition_count"] = max(1, spec["map_job_count"] / 2)
            spec["min_data_size_per_map_job"] = config.MIN_SIZE_PER_JOB

    if strategy is None: strategy = config.DEFAULT_STRATEGY
    sort_by = _prepare_sort_by(sort_by)
    reduce_by = _prepare_reduce_by(reduce_by)
    input_format, output_format = _prepare_formats(format, input_format, output_format)

    run_map_reduce.spec = {}
    run_map_reduce.files_to_remove = []
    def prepare_operation(binary, files, file_paths, spec_keyword):
        """ Returns new spec """
        if binary is None: return
        if file_paths is None: file_paths = []
        files = _prepare_files(files)
        binary = _prepare_binary(binary, files)
        run_map_reduce.spec = update(run_map_reduce.spec,
            {
                spec_keyword: {
                    "input_format": input_format.to_json(),
                    "output_format": output_format.to_json(),
                    "command": binary,
                    "file_paths": flatten(files + file_paths),
                    "memory_limit": config.MEMORY_LIMIT
                }
            })
        run_map_reduce.files_to_remove += files

    reducer = _add_output_fd_redirect(reducer, len(destination_table))
    prepare_operation(mapper, map_files, map_file_paths, "mapper")
    prepare_operation(reducer, reduce_files, reduce_file_paths, "reducer")

    source_table = _prepare_source_tables(source_table)
    destination_table = _prepare_destination_tables(destination_table)

    if table_writer is not None:
        for job_io in ["map_job_io", "reduce_job_io", "sort_job_io"]:
            run_map_reduce.spec[job_io] = {}
            run_map_reduce.spec[job_io]["table_writer"] = table_writer

    run_map_reduce.spec = update(
        run_map_reduce.spec,
        {"sort_by": flatten(sort_by),
         "reduce_by": flatten(reduce_by),
         "input_table_paths": map(get_yson_name, source_table),
         "output_table_paths": map(get_output_yson_name, destination_table)
        })

    if spec is not None:
        run_map_reduce.spec = update(run_map_reduce.spec, spec)

    prepare_job_count(run_map_reduce.spec, source_table)
    params = \
        add_user_spec(add_transaction_params(
            {"spec": run_map_reduce.spec}))
    operation = make_request("map_reduce", params)
    strategy.process_operation("map_reduce", operation,
         Finalizer(run_map_reduce.files_to_remove, destination_table))

def run_operation(binary, source_table, destination_table,
                  files=None, file_paths=None,
                  format=None, input_format=None, output_format=None,
                  strategy=None,
                  table_writer=None, spec=None,
                  op_type=None,
                  reduce_by=None):
    def prepare_job_count(spec, source_table):
        if "job_count" not in spec:
            spec["job_count"] = _calc_job_count(source_table)
            spec["min_data_size_per_job"] = config.MIN_SIZE_PER_JOB


    input_format, output_format = _prepare_formats(format, input_format, output_format)
    files = _prepare_files(files)
    binary = _prepare_binary(binary, files)

    if file_paths is None: file_paths = []
    file_paths += files

    source_table = _prepare_source_tables(source_table)
    if op_type == "reduce":
        are_input_tables_sorted =  all(
            is_prefix(_prepare_reduce_by(reduce_by), get_sorted_by(table.name))
            for table in source_table)
        if reduce_by is None:
            reduce_by = sort_by = None
        else:
            sort_by = reduce_by
        if not are_input_tables_sorted:
            run_map_reduce(
                mapper=None,
                reducer=binary,
                source_table=source_table,
                destination_table=destination_table,
                reduce_file_paths=file_paths,
                input_format=input_format, output_format=output_format,
                reduce_by=reduce_by,
                sort_by=sort_by,
                spec=spec)
            return
        else:
            reduce_by = _prepare_reduce_by(reduce_by)

    destination_table = _prepare_destination_tables(destination_table)

    binary = _add_output_fd_redirect(binary, len(destination_table))

    operation_descr = \
                {"command": binary,
                 "file_paths": file_paths,
                 "input_format": input_format.to_json(),
                 "output_format": output_format.to_json(),
                 "memory_limit": config.MEMORY_LIMIT}
    if op_type == "reduce":
        operation_descr.update({"reduce_by": reduce_by})

    if spec is None: spec = {}
    if table_writer is not None:
        spec = update({"job_io": {"table_writer": table_writer}}, spec)

    op_key = None
    if op_type == "map": op_key = "mapper"
    if op_type == "reduce": op_key = "reducer"
    spec = update(
        {"input_table_paths": map(get_yson_name, source_table),
         "output_table_paths": map(get_output_yson_name, destination_table),
         op_key: operation_descr},
        spec)

    prepare_job_count(spec, source_table)
    params = \
        add_user_spec(add_transaction_params(
            {"spec": spec}))
    operation = make_request(op_type, params)

    if strategy is None: strategy = config.DEFAULT_STRATEGY
    strategy.process_operation(op_type, operation, Finalizer(files, destination_table))

def run_map(binary, source_table, destination_table, **kwargs):
    kwargs["op_type"] = "map"
    run_operation(binary, source_table, destination_table, **kwargs)

def run_reduce(binary, source_table, destination_table, **kwargs):
    kwargs["op_type"] = "reduce"
    run_operation(binary, source_table, destination_table, **kwargs)

