import config
import py_wrapper
from common import flatten, require, unlist, update, EMPTY_GENERATOR, parse_bool, \
                   is_prefix, get_value, compose, execute_handling_sigint, bool_to_string, \
                   chunk_iter_lines
from errors import YtError
from version import VERSION
from driver import read_content, get_host_for_heavy_operation
from table import TablePath, to_table, to_name, prepare_path
from tree_commands import exists, remove, remove_with_empty_dirs, get_attribute, copy, \
                          move, mkdir, find_free_subpath, create, get_type, \
                          _make_formatted_transactional_request
from file_commands import smart_upload_file
from transaction_commands import _make_transactional_request, abort_transaction
from transaction import PingableTransaction
from format import Format
from lock import lock
from heavy_commands import make_heavy_request
from http import NETWORK_ERRORS
import yt.logger as logger

from yt.yson import yson_to_json

import os
import sys
import types
import exceptions
import simplejson as json
from cStringIO import StringIO

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
    tables = map(to_table, flatten(tables))
    if config.TREAT_UNEXISTING_AS_EMPTY:
        tables = _filter_empty_tables(tables)
    return tables

def _check_columns(columns, type):
    if len(columns) == 1 and "," in columns:
        logger.info('Comma found in column name "%s". '
                    'Did you mean to %s by a composite key?',
                    columns[0], type)

def _prepare_reduce_by(reduce_by):
    if reduce_by is None:
        if config.USE_YAMR_SORT_REDUCE_COLUMNS:
            reduce_by = ["key"]
        else:
            raise YtError("reduce_by option is required")
    reduce_by = flatten(reduce_by)
    _check_columns(reduce_by, "reduce")
    return reduce_by

def _prepare_sort_by(sort_by):
    if sort_by is None:
        if config.USE_YAMR_SORT_REDUCE_COLUMNS:
            sort_by = ["key", "subkey"]
        else:
            raise YtError("sort_by option is required")
    sort_by = flatten(sort_by)
    _check_columns(sort_by, "sort")
    return sort_by

def _prepare_files(files):
    if files is None:
        return []

    file_paths = []
    for file in flatten(files):
        file_paths.append(smart_upload_file(file))
    return file_paths

def _prepare_formats(format, input_format, output_format):
    if format is None: format = config.format.TABULAR_DATA_FORMAT
    if isinstance(format, str):
        format = Format(format)

    if input_format is None: input_format = format
    require(input_format is not None,
            YtError("You should specify input format"))
    if output_format is None: output_format = format
    require(output_format is not None,
            YtError("You should specify output format"))
    return input_format, output_format

def _prepare_format(format):
    if format is None: format = config.format.TABULAR_DATA_FORMAT
    if isinstance(format, str):
        format = Format(format)

    require(format is not None,
            YtError("You should specify format"))
    return format

def _prepare_binary(binary, operation_type, input_format=None, output_format=None, reduce_by=None):
    if isinstance(binary, types.FunctionType) or hasattr(binary, "__call__"):
        binary, binary_file, files = py_wrapper.wrap(binary, operation_type, input_format, output_format, reduce_by)
        uploaded_files = _prepare_files([binary_file] + files)
        if config.REMOVE_TEMP_FILES:
            for file in files:
                os.remove(file)
        return binary, uploaded_files
    else:
        return binary, []

def _prepare_destination_tables(tables, replication_factor, compression_codec):
    if tables is None:
        if config.THROW_ON_EMPTY_DST_LIST:
            raise YtError("Destination tables are absent")
        return []
    tables = map(to_table, flatten(tables))
    for table in tables:
        if exists(table.name):
            compression_codec_ok = (compression_codec is None) or (compression_codec == get_attribute(table.name, "compression_codec"))
            replication_factor_ok = (replication_factor is None) or (replication_factor == get_attribute(table.name, "replication_factor"))
            require(compression_codec_ok and replication_factor_ok,
                    YtError("Cannot append to table %s and set replication factor "
                            "or compression codec" % table))
        else:
            create_table(table.name, ignore_existing=True,
                         replication_factor=replication_factor, compression_codec=compression_codec)
    return tables

def _remove_locks(table):
    for lock_obj in get_attribute(table, "locks", []):
        if lock_obj["mode"] != "snapshot":
            if exists("//sys/transactions/" + lock_obj["transaction_id"]):
                abort_transaction(lock_obj["transaction_id"])

def _remove_tables(tables):
    for table in tables:
        if exists(table) and get_type(table) == "table" and not table.append:
            if config.FORCE_DROP_DST:
                _remove_locks(table)
            remove(table)

def _add_user_command_spec(op_type, binary, input_format, output_format, files, file_paths, memory_limit, reduce_by, spec):
    if binary is None:
        return spec, []
    files = _prepare_files(files)
    binary, additional_files = _prepare_binary(binary, op_type, input_format, output_format, reduce_by)
    spec = update(
        {
            op_type: {
                "input_format": input_format.json(),
                "output_format": output_format.json(),
                "command": binary,
                "file_paths": map(
                    yson_to_json,
                    flatten(files + additional_files + map(prepare_path, get_value(file_paths, [])))
                ),
                "use_yamr_descriptors": bool_to_string(config.USE_MAPREDUCE_STYLE_DESTINATION_FDS)
            }
        },
        spec)

    memory_limit = get_value(memory_limit, config.MEMORY_LIMIT)
    if memory_limit is not None:
        spec = update({op_type: {"memory_limit": int(memory_limit)}}, spec)
    if config.POOL is not None:
        spec = update({"pool": config.POOL}, spec)

    return spec, files + additional_files

def _add_user_spec(spec):
    return update({"wrapper_version": VERSION}, spec)

def _add_input_output_spec(source_table, destination_table, spec):
    def get_input_name(table):
        return table.get_json()
    def get_output_name(table):
        return table.get_json()

    spec = update({"input_table_paths": map(get_input_name, source_table)}, spec)
    if isinstance(destination_table, TablePath):
        spec = update({"output_table_path": get_output_name(destination_table)}, spec)
    else:
        spec = update({"output_table_paths": map(get_output_name, destination_table)}, spec)
    return spec

def _add_table_writer_spec(job_types, table_writer, spec):
    if table_writer is not None:
        for job_type in flatten(job_types):
            spec = update({job_type: {"table_writer": table_writer}}, spec)
    return spec

def _make_operation_request(command_name, spec, strategy, finalizer=None, verbose=False):
    def run_operation(finalizer):
        operation = _make_formatted_transactional_request(command_name, {"spec": spec}, format=None, verbose=verbose)
        get_value(strategy, config.DEFAULT_STRATEGY).process_operation(command_name, operation, finalizer)

    if not config.DETACHED:
        transaction = PingableTransaction(
            config.OPERATION_TRANSACTION_TIMEOUT,
            attributes={"title": "Python wrapper: envelope transaction of operation"})
        def run_in_transaction():
            def envelope_finalizer(state):
                if finalizer is not None:
                    finalizer(state)
                transaction.__exit__(*sys.exc_info())
            with transaction:
                run_operation(envelope_finalizer)

        def finish_transaction():
            transaction.__exit__(*sys.exc_info())

        execute_handling_sigint(run_in_transaction, finish_transaction)
    else:
        run_operation(finalizer)

""" Common table methods """
def create_table(path, recursive=None, ignore_existing=False, replication_factor=None, compression_codec=None, attributes=None):
    """ Creates empty table, use recursive for automatically creation the path """
    table = TablePath(path)
    recursive = get_value(recursive, config.CREATE_RECURSIVE)
    attributes = get_value(attributes, {})
    if replication_factor is not None:
        attributes["replication_factor"] = replication_factor
    if compression_codec is not None:
        attributes["compression_codec"] = compression_codec
    create("table", table.name, recursive=recursive, ignore_existing=ignore_existing, attributes=attributes)

def create_temp_table(path=None, prefix=None):
    """ Creates temporary table by given path with given prefix """
    if path is None:
        path = config.TEMP_TABLES_STORAGE
        mkdir(path, recursive=True)
    else:
        path = to_name(path)
    require(exists(path), YtError("You cannot create table in unexisting path"))
    if prefix is not None:
        path = os.path.join(path, prefix)
    else:
        if not path.endswith("/"):
            path = path + "/"
    name = find_free_subpath(path)
    create_table(name)
    return name


def write_table(table, input_stream, format=None, table_writer=None, replication_factor=None, compression_codec=None):
    """
    Writes rows from input_stream to table.

    Input stream may be python file-like object or string, or list of strings. You also can
    use StringIterIO to wrap iter of strings.

    There are two modes.
    In chunk mode we write by portion of fixed size. Each portion is written with retries.
    In single mode we write all stream as is through HTTP.

    In both cases Transer-Encoding is used.
    """
    table = to_table(table)
    format = _prepare_format(format)

    params = {}
    params["input_format"] = format.json()
    if table_writer is not None:
        params["table_writer"] = table_writer

    def split_rows(stream):
        while True:
            row = format.read_row(stream)
            if row:
                yield row
            else:
                return

    def prepare_table(path):
        if exists(path):
            require(replication_factor is None and compression_codec is None,
                    YtError("Cannot write to existing path %s with set replication factor or compression codec" % path))
        else:
            create_table(path, ignore_existing=True,
                         replication_factor=replication_factor, compression_codec=compression_codec)

    can_split_input = True
    if isinstance(input_stream, types.ListType):
        input_stream = iter(input_stream)
    elif isinstance(input_stream, file) or \
            (hasattr(input_stream, "read") and  hasattr(input_stream, "readline")):
        if format.is_read_row_supported():
            input_stream = split_rows(input_stream)
        else:
            can_split_input = False
    elif isinstance(input_stream, str):
        input_stream = split_rows(StringIO(input_stream))

    if config.USE_RETRIES_DURING_WRITE and can_split_input:
        input_stream = chunk_iter_lines(input_stream, config.CHUNK_SIZE)

    if config.USE_RETRIES_DURING_WRITE and not can_split_input:
        logger.warning("Cannot split input into rows. Write is processing by one request.")

    make_heavy_request(
        "write",
        input_stream,
        table,
        params,
        prepare_table,
        config.USE_RETRIES_DURING_WRITE and can_split_input)

    if config.TREAT_UNEXISTING_AS_EMPTY and is_empty(table):
        _remove_tables([table])


def read_table(table, format=None, table_reader=None, response_type=None):
    """
    Downloads file from path.
    Response type means the output format. By default it is line generator.
    """
    table = to_table(table)
    format = _prepare_format(format)
    if config.TREAT_UNEXISTING_AS_EMPTY and not exists(table.name):
        return EMPTY_GENERATOR

    params = {
        "path": table.get_json(),
        "output_format": format.json()
    }
    if table_reader is not None:
        params["table_reader"] = table_reader

    if not config.RETRY_READ:
        response = _make_transactional_request(
            "read",
            params,
            proxy=get_host_for_heavy_operation(),
            return_raw_response=True)

        return read_content(response, get_value(response_type, "iter_lines"))
    else:
        title = "Python wrapper: read {0}".format(to_name(table))
        tx = PingableTransaction(timeout=config.HEAVY_COMMAND_TRANSACTION_TIMEOUT,
                                 attributes={"title": title})
        tx.__enter__()

        class Index(object):
            def __init__(self, index):
                self.index = index

            def get(self):
                return self.index

            def increment(self):
                self.index += 1

        def run_with_retries(func):
            for i in xrange(config.READ_RETRY_COUNT):
                try:
                    return func()
                except tuple(list(NETWORK_ERRORS)) as err:
                    logger.warning("Retry %d failed with message %s", i + 1, str(err))
                    if i + 1 == config.READ_RETRY_COUNT:
                        raise

        def iter_with_retries(iter):
            try:
                for i in xrange(config.READ_RETRY_COUNT):
                    try:
                        for elem in iter:
                            yield elem
                    except tuple(list(NETWORK_ERRORS)) as err:
                        logger.warning("Retry %d failed with message %s", i + 1, str(err))
                        if i + 1 == config.READ_RETRY_COUNT:
                            raise
            except exceptions.GeneratorExit:
                tx.__exit__(None, None, None)
            except:
                tx.__exit__(*sys.exc_info())
                raise
            else:
                tx.__exit__(None, None, None)

        def get_start_row_index():
            response = _make_transactional_request(
                "read",
                params,
                proxy=get_host_for_heavy_operation(),
                return_raw_response=True)
            rsp_params = json.loads(response.headers["X-YT-Response-Parameters"])
            return rsp_params.get("start_row_index", None)


        def read_iter(index):
            table.name.attributes["lower_limit"] = {"row_index": index.get()}
            params["path"] = table.get_json()
            response = _make_transactional_request(
                "read",
                params,
                proxy=get_host_for_heavy_operation(),
                return_raw_response=True)
            for record in read_content(response, get_value(response_type, "iter_lines")):
                yield record
                index.increment()

        try:
            lock(table, mode="snapshot")
            index = Index(run_with_retries(get_start_row_index))
            if index.get() is None:
                tx.__exit__(None, None, None)
                return (_ for _ in [])
            return iter_with_retries(read_iter(index))
        except:
            tx.__exit__(*sys.exc_info())
            raise


def _are_nodes(source_tables, destination_table):
    return len(source_tables) == 1 and not source_tables[0].has_delimiters() and not destination_table.append

def copy_table(source_table, destination_table, replace=True):
    """
    Copy table. Source table may be a list of tables. In this case tables would
    be merged.
    """
    if config.REPLACE_TABLES_WHILE_COPY_OR_MOVE: replace = True
    source_tables = _prepare_source_tables(source_table)
    if config.TREAT_UNEXISTING_AS_EMPTY and len(source_tables) == 0:
        return
    destination_table = to_table(destination_table)
    if _are_nodes(source_tables, destination_table):
        if replace and exists(destination_table.name) and to_name(source_tables[0]) != to_name(destination_table):
            # in copy destination should be absent
            remove(destination_table.name)
        dirname = os.path.dirname(destination_table.name)
        if dirname != "//":
            mkdir(dirname, recursive=True)
        copy(source_tables[0].name, destination_table.name)
    else:
        source_names = [table.name for table in source_tables]
        mode = "sorted" if (all(map(is_sorted, source_names)) and not destination_table.append) else "ordered"
        run_merge(source_tables, destination_table, mode)

def move_table(source_table, destination_table, replace=True):
    """
    Move table. Source table may be a list of tables. In this case tables would
    be merged.
    """
    if config.REPLACE_TABLES_WHILE_COPY_OR_MOVE: replace = True
    source_tables = _prepare_source_tables(source_table)
    if config.TREAT_UNEXISTING_AS_EMPTY and len(source_tables) == 0:
        return
    destination_table = to_table(destination_table)
    if _are_nodes(source_tables, destination_table):
        if source_tables[0] == destination_table:
            return
        if replace and exists(destination_table.name):
            remove(destination_table.name)
        move(source_tables[0].name, destination_table.name)
    else:
        copy_table(source_table, destination_table)
        for table in source_tables:
            if to_name(table) == to_name(destination_table):
                continue
            remove(table.name)

def run_erase(table, strategy=None):
    """
    Erase table. It differs from remove command.
    Erase only remove given content. You can erase range
    of records in the table.
    """
    table = to_table(table)
    if config.TREAT_UNEXISTING_AS_EMPTY and not exists(table.name):
        return
    _make_operation_request("erase", {"table_path": table.get_json()}, strategy)

def records_count(table):
    """Return number of records in the table"""
    table = to_name(table)
    if config.TREAT_UNEXISTING_AS_EMPTY and not exists(table):
        return 0
    return get_attribute(table, "row_count")

def is_empty(table):
    """Check table for the emptiness"""
    return records_count(to_name(table)) == 0

def get_sorted_by(table, default=None):
    if default is None:
        default = [] if config.TREAT_UNEXISTING_AS_EMPTY else None
    return get_attribute(to_name(table), "sorted_by", default=default)

def is_sorted(table):
    """Checks that table is sorted"""
    if config.USE_YAMR_SORT_REDUCE_COLUMNS:
        return get_sorted_by(table, []) == ["key", "subkey"]
    else:
        return parse_bool(get_attribute(to_name(table), "sorted", default="false"))

def run_merge(source_table, destination_table, mode=None,
              strategy=None, table_writer=None,
              replication_factor=None, compression_codec=None,
              job_count=None, spec=None):
    """
    Merge source tables and write it to destination table.
    Mode should be 'unordered', 'ordered', or 'sorted'.
    """
    source_table = _prepare_source_tables(source_table)
    destination_table = unlist(_prepare_destination_tables(destination_table, replication_factor, compression_codec))

    spec = compose(
        _add_user_spec,
        lambda _: _add_table_writer_spec("job_io", table_writer, _),
        lambda _: _add_input_output_spec(source_table, destination_table, _),
        lambda _: update({"job_count": job_count}, _) if job_count is not None else _,
        lambda _: update({"mode": get_value(mode, "unordered")}, _),
        lambda _: get_value(_, {})
    )(spec)

    _make_operation_request("merge", spec, strategy, finalizer=None)

def run_sort(source_table, destination_table=None, sort_by=None,
             strategy=None, table_writer=None, replication_factor=None,
             compression_codec=None, spec=None):
    """
    Sort source table. If destination table is not specified, than it equals to source table.
    """

    sort_by = _prepare_sort_by(sort_by)
    source_table = _prepare_source_tables(source_table)
    for table in source_table:
        require(exists(table.name), YtError("Table %s should exist" % table))
    if all(is_prefix(sort_by, get_sorted_by(table.name, [])) for table in source_table):
        #(TODO) Hack detected: make something with it
        if len(source_table) > 0:
            run_merge(source_table, destination_table, "sorted",
                      strategy=strategy, table_writer=table_writer, spec=spec)
        return

    if destination_table is None:
        require(len(source_table) == 1 and not source_table[0].has_delimiters(),
                YtError("You must specify destination sort table "
                        "in case of multiple source tables"))
        destination_table = source_table[0]
    destination_table = unlist(_prepare_destination_tables(destination_table, replication_factor, compression_codec))

    if config.TREAT_UNEXISTING_AS_EMPTY and not source_table:
        _remove_tables([destination_table])
        return

    spec = compose(
        _add_user_spec,
        lambda _: _add_table_writer_spec(["sort_job_io", "merge_job_io"], table_writer, _),
        lambda _: _add_input_output_spec(source_table, destination_table, _),
        lambda _: update({"sort_by": sort_by}, _),
        lambda _: get_value(_, {})
    )(spec)

    _make_operation_request("sort", spec, strategy, finalizer=None)

""" Map and reduce methods """
class Finalizer(object):
    def __init__(self, files, output_tables):
        self.files = files if files is not None else []
        self.output_tables = output_tables

    def __call__(self, state):
        if state == "completed":
            for table in map(to_name, self.output_tables):
                self.check_for_merge(table)
        if config.DELETE_EMPTY_TABLES:
            for table in map(to_name, self.output_tables):
                if is_empty(table):
                    remove_with_empty_dirs(table)
        if config.REMOVE_TEMP_FILES:
            for file in self.files:
                remove(file, force=True)

    def check_for_merge(self, table):
        chunk_count = int(get_attribute(table, "chunk_count"))
        if  chunk_count < config.MIN_CHUNK_COUNT_FOR_MERGE_WARNING:
            return

        # We use uncompressed data size to simplify recommended command
        chunk_size = float(get_attribute(table, "compressed_data_size")) / chunk_count
        if chunk_size > config.MAX_CHUNK_SIZE_FOR_MERGE_WARNING:
            return

        data_size_per_job = min(
                16 * 1024 * config.MB,
                int(500 * config.MB / float(get_attribute(table, "compression_ratio"))))

        mode = "sorted" if is_sorted(table) else "unordered"

        if config.MERGE_INSTEAD_WARNING:
            run_merge(src=table, dst=table, mode=mode,
                      spec={"combine_chunks": "true", "data_size_per_job": data_size_per_job})
        else:
            logger.info("Chunks of output table {0} are too small. "
                        "This may cause suboptimal system performance. "
                        "If this table is not temporary then consider running the following command:\n"
                        "yt merge --mode {1} --proxy {3} --src {0} --dst {0} "
                        "--spec '{{"
                           "combine_chunks=true;"
                           "data_size_per_job={2}"
                        "}}'".format(table, mode, data_size_per_job, config.http.PROXY))

def run_map_reduce(mapper, reducer, source_table, destination_table,
                   format=None,
                   map_input_format=None, map_output_format=None,
                   reduce_input_format=None, reduce_output_format=None,
                   strategy=None, table_writer=None, spec=None,
                   replication_factor=None, compression_codec=None,
                   map_files=None, reduce_files=None,
                   map_file_paths=None, reduce_file_paths=None,
                   mapper_memory_limit=None, reducer_memory_limit=None,
                   sort_by=None, reduce_by=None,
                   reduce_combiner=None,
                   reduce_combiner_input_format=None, reduce_combiner_output_format=None,
                   reduce_combiner_files=None, reduce_combiner_file_paths=None,
                   reduce_combiner_memory_limit=None):

    run_map_reduce.files_to_remove = []
    def memorize_files(spec, files):
        run_map_reduce.files_to_remove += files
        return spec

    source_table = _prepare_source_tables(source_table)
    destination_table = _prepare_destination_tables(destination_table, replication_factor, compression_codec)

    if config.TREAT_UNEXISTING_AS_EMPTY and not source_table:
        _remove_tables(destination_table)
        return

    map_input_format, map_output_format = _prepare_formats(format, map_input_format, map_output_format)
    reduce_input_format, reduce_output_format = _prepare_formats(format, reduce_input_format, reduce_output_format)
    reduce_combiner_input_format, reduce_combiner_output_format = _prepare_formats(format, reduce_combiner_input_format, reduce_combiner_output_format)

    if sort_by is None:
        sort_by = reduce_by

    spec = compose(
        _add_user_spec,
        lambda _: _add_table_writer_spec(["map_job_io", "reduce_job_io", "sort_job_io"], table_writer, _),
        lambda _: _add_input_output_spec(source_table, destination_table, _),
        lambda _: update({"sort_by": _prepare_sort_by(sort_by),
                          "reduce_by": _prepare_reduce_by(reduce_by)}, _),
        lambda _: memorize_files(*_add_user_command_spec("mapper", mapper, map_input_format, map_output_format, map_files, map_file_paths, mapper_memory_limit, None, _)),
        lambda _: memorize_files(*_add_user_command_spec("reducer", reducer, reduce_input_format, reduce_output_format, reduce_files, reduce_file_paths, reducer_memory_limit, reduce_by, _)),
        lambda _: memorize_files(*_add_user_command_spec("reduce_combiner", reduce_combiner, reduce_combiner_input_format, reduce_combiner_output_format, reduce_combiner_files, reduce_combiner_file_paths, reduce_combiner_memory_limit, reduce_by, _)),
        lambda _: get_value(_, {})
    )(spec)

    _make_operation_request("map_reduce", spec, strategy, Finalizer(run_map_reduce.files_to_remove, destination_table))

def run_operation(binary, source_table, destination_table,
                  files=None, file_paths=None,
                  format=None, input_format=None, output_format=None,
                  strategy=None,
                  table_writer=None,
                  replication_factor=None,
                  compression_codec=None,
                  job_count=None,
                  memory_limit=None,
                  spec=None,
                  op_name=None,
                  reduce_by=None):
    run_operation.files = []
    def memorize_files(spec, files):
        run_operation.files += files
        return spec

    source_table = _prepare_source_tables(source_table)
    if op_name == "reduce":
        if config.RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED:
            are_input_tables_sorted =  all(
                is_prefix(_prepare_reduce_by(reduce_by), get_sorted_by(table.name, []))
                for table in source_table)
            if not are_input_tables_sorted:
                if job_count is not None:
                    spec = update({"partition_count": job_count}, spec)
                run_map_reduce(
                    mapper=None,
                    reducer=binary,
                    reduce_files=files,
                    reduce_file_paths=file_paths,
                    source_table=source_table,
                    destination_table=destination_table,
                    format=format,
                    reduce_input_format=input_format,
                    reduce_output_format=output_format,
                    table_writer=table_writer,
                    reduce_by=reduce_by,
                    sort_by=reduce_by,
                    replication_factor=replication_factor,
                    compression_codec=compression_codec,
                    reducer_memory_limit=memory_limit,
                    strategy=strategy,
                    spec=spec)
                return
            else:
                reduce_by = _prepare_reduce_by(reduce_by)
        else:
            reduce_by = _prepare_reduce_by(reduce_by)

    destination_table = _prepare_destination_tables(destination_table, replication_factor, compression_codec)
    input_format, output_format = _prepare_formats(format, input_format, output_format)

    if config.TREAT_UNEXISTING_AS_EMPTY and not source_table:
        _remove_tables(destination_table)
        return

    op_type = None
    if op_name == "map": op_type = "mapper"
    if op_name == "reduce": op_type = "reducer"

    spec = compose(
        _add_user_spec,
        lambda _: _add_table_writer_spec("job_io", table_writer, _),
        lambda _: _add_input_output_spec(source_table, destination_table, _),
        lambda _: update({"reduce_by": _prepare_reduce_by(reduce_by)}, _) if op_name == "reduce" else _,
        lambda _: update({"job_count": job_count}, _) if job_count is not None else _,
        lambda _: update({"memory_limit": memory_limit}, _) if memory_limit is not None else _,
        lambda _: memorize_files(*_add_user_command_spec(op_type, binary, input_format, output_format, files, file_paths, memory_limit, reduce_by, _)),
        lambda _: get_value(_, {})
    )(spec)

    _make_operation_request(op_name, spec, strategy, Finalizer(run_operation.files, destination_table))

def run_map(binary, source_table, destination_table, **kwargs):
    kwargs["op_name"] = "map"
    run_operation(binary, source_table, destination_table, **kwargs)

def run_reduce(binary, source_table, destination_table, **kwargs):
    kwargs["op_name"] = "reduce"
    run_operation(binary, source_table, destination_table, **kwargs)

