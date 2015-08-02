"""
Commands for table working and Map-Reduce operations.
.. seealso:: `operations on wiki <https://wiki.yandex-team.ru/yt/userdoc/operations>`_

Python wrapper has some improvements over bare YT operations:

* upload files automatically

* create or erase output table

* delete files after

**Heavy commands** have deprecated parameter `response_type`:  one of \
"iter_lines" (default), "iter_content", "raw", "string". It specifies response type.

.. _operation_parameters:

Common operations parameters
-----------------------


* **spec** : (dict) universal method to set operation parameters

* **strategy** : (`yt.wrapper.operation_commands.WaitStrategy`) (Deprecated!) \
strategy of waiting result, `yt.wrapper.get_config(client).DEFAULT_STRATEGY` by default

* **replication_factor** : (integer) number of output data replicas

* **compression_codec** : (one of "none" (default for files), "lz4" (default for tables), "snappy",\
 "gzip_best_compression", "gzip_normal", "lz4_high_compresion", "quick_lz") compression \
algorithm for output data

* **job_count** : (integer) recommendation how many jobs should run

* **table_writer** : (dict) spec of `"write" operation \
<https://wiki.yandex-team.ru/yt/Design/ClientInterface/Core#write>`_.

* **table_reader** : (dict) spec of `"read" operation \
<https://wiki.yandex-team.ru/yt/Design/ClientInterface/Core#read>`_.

* **format** : (string or descendant of `yt.wrapper.format.Format`) format of input and output \
data of operation

* **memory_limit** : (integer) memory limit in Mb in *scheduler* for every *job* (512Mb by default)


Operation run under self-pinged transaction, if `yt.wrapper.get_config(client)["detached"]` is `False`.
"""

import config
from config import get_config, get_option
import py_wrapper
from common import flatten, require, unlist, update, parse_bool, is_prefix, get_value, \
                   compose, bool_to_string, chunk_iter_lines, chunk_iter_stream, get_version, MB, EMPTY_GENERATOR
from errors import YtIncorrectResponse, YtError, YtOperationFailedError
from driver import make_request, get_backend_type
from keyboard_interrupts_catcher import KeyboardInterruptsCatcher
from table import TablePath, to_table, to_name, prepare_path
from cypress_commands import exists, remove, remove_with_empty_dirs, get_attribute, copy, \
                             move, mkdir, find_free_subpath, create, get, get_type, set_attribute, \
                             _make_formatted_transactional_request, has_attribute
from file_commands import smart_upload_file
from operation_commands import Operation, WaitStrategy
from transaction_commands import _make_transactional_request, abort_transaction
from transaction import Transaction, Abort
from format import create_format, YsonFormat, YamrFormat
from lock import lock
from heavy_commands import make_write_request, make_read_request
from http import RETRIABLE_ERRORS, get_api_version, HTTPError
from response_stream import ResponseStream, EmptyResponseStream
import yt.logger as logger
import yt.packages.simplejson as json

import os
import sys
import types
import random
import exceptions
import tempfile
import socket
import getpass
from cStringIO import StringIO
from copy import deepcopy

# Auxiliary methods

DEFAULT_EMPTY_TABLE = TablePath("//sys/empty_yamr_table", simplify=False)

def _split_rows(stream, format, raw):
    if isinstance(stream, str):
        stream = StringIO(stream)

    if hasattr(stream, "read"):
        if not raw:
            raise YtError("Passing raw=False and stream as input forbidden")
        for row in format.load_rows(stream, raw=True):
            yield row
    else:
        for row in stream:
            if raw:
                yield row
            else:
                yield format.dumps_row(row)


def _prepare_source_tables(tables, replace_unexisting_by_empty=True, client=None):
    result = [to_table(table, client=client) for table in flatten(tables)]
    if not result:
        raise YtError("You must specify non-empty list of source tables")
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"]:
        filtered_result = []
        for table in result:
            if exists(table.name, client=client):
                filtered_result.append(table)
            else:
                logger.warning("Warning: input table '%s' does not exist", table.name)
                if replace_unexisting_by_empty:
                    filtered_result.append(DEFAULT_EMPTY_TABLE)
        result = filtered_result
    return result

def _are_default_empty_table(tables):
    return all(table == DEFAULT_EMPTY_TABLE for table in tables)

def _check_columns(columns, type):
    if len(columns) == 1 and "," in columns:
        logger.info('Comma found in column name "%s". '
                    'Did you mean to %s by a composite key?',
                    columns[0], type)

def _prepare_reduce_by(reduce_by, client):
    if reduce_by is None:
        if get_config(client)["yamr_mode"]["use_yamr_sort_reduce_columns"]:
            reduce_by = ["key"]
        else:
            raise YtError("reduce_by option is required")
    reduce_by = flatten(reduce_by)
    _check_columns(reduce_by, "reduce")
    return reduce_by

def _prepare_sort_by(sort_by, client):
    if sort_by is None:
        if get_config(client)["yamr_mode"]["use_yamr_sort_reduce_columns"]:
            sort_by = ["key", "subkey"]
        else:
            raise YtError("sort_by option is required")
    sort_by = flatten(sort_by)
    _check_columns(sort_by, "sort")
    return sort_by

def _reliably_upload_files(files, client=None):
    if files is None:
        return []

    file_paths = []
    with Transaction(null=True, client=client):
        for file in flatten(files):
            file_paths.append(smart_upload_file(file, client=client))
    return file_paths

def _is_python_function(binary):
    return isinstance(binary, types.FunctionType) or hasattr(binary, "__call__")

def _prepare_formats(format, input_format, output_format, binary, client):
    if format is None:
        format = get_config(client)["tabular_data_format"]
    if format is None and _is_python_function(binary):
        format = YsonFormat()
    if isinstance(format, str):
        format = create_format(format)
    if isinstance(input_format, str):
        input_format = create_format(input_format)
    if isinstance(output_format, str):
        output_format = create_format(output_format)

    if input_format is None:
        input_format = format
    require(input_format is not None,
            YtError("You should specify input format"))

    if output_format is None:
        output_format = format
    require(output_format is not None,
            YtError("You should specify output format"))

    return input_format, output_format

def _prepare_format(format, raw, client):
    if format is None:
        format = get_config(client)["tabular_data_format"]
    if not raw and format is None:
        format = YsonFormat(process_table_index=False)
    if isinstance(format, str):
        format = create_format(format)

    require(format is not None,
            YtError("You should specify format"))
    return format

class TempfilesManager(object):
    def __init__(self, client):
        self.client = client
        self._tempfiles_pool = []

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if get_config(self.client)["clear_local_temp_files"]:
            for file in self._tempfiles_pool:
                os.remove(file)

    def create_tempfile(self, *args, **kwargs):
        """Use syntax tempfile.mkstemp"""
        fd, filepath = tempfile.mkstemp(*args, **kwargs)
        os.close(fd)
        self._tempfiles_pool.append(filepath)
        return filepath

def _prepare_binary(binary, operation_type, input_format=None, output_format=None,
                    reduce_by=None, client=None):
    if _is_python_function(binary):
        if isinstance(input_format, YamrFormat) and reduce_by is not None and set(reduce_by) != set(["key"]):
            raise YtError("Yamr format does not support reduce by %r", reduce_by)
        with TempfilesManager(client) as tempfiles_manager:
            binary, binary_file, files = py_wrapper.wrap(binary, operation_type, tempfiles_manager,
                                                         input_format, output_format, reduce_by, client=client)
            uploaded_files = _reliably_upload_files([binary_file] + files, client=client)
            return binary, uploaded_files
    else:
        return binary, []

def _prepare_destination_tables(tables, replication_factor, compression_codec, client=None):
    if tables is None:
        if get_config(client)["yamr_mode"]["throw_on_missing_destination"]:
            raise YtError("Destination tables are missing")
        return []
    tables = map(lambda name: to_table(name, client=client), flatten(tables))
    for table in tables:
        if exists(table.name, client=client):
            compression_codec_ok = (compression_codec is None) or \
                                   (compression_codec == get_attribute(table.name,
                                                                       "compression_codec",
                                                                       client=client))
            replication_factor_ok = (replication_factor is None) or \
                                    (replication_factor == get_attribute(table.name,
                                                                         "replication_factor",
                                                                         client=client))
            require(compression_codec_ok and replication_factor_ok,
                    YtError("Cannot append to table %s and set replication factor "
                            "or compression codec" % table))
        else:
            create_table(table.name, ignore_existing=True, replication_factor=replication_factor,
                         compression_codec=compression_codec, client=client)
    return tables

def _remove_locks(table, client=None):
    for lock_obj in get_attribute(table, "locks", [], client=client):
        if lock_obj["mode"] != "snapshot":
            if exists("//sys/transactions/" + lock_obj["transaction_id"], client=client):
                abort_transaction(lock_obj["transaction_id"], client=client)

def _remove_tables(tables, client=None):
    for table in tables:
        if exists(table) and get_type(table) == "table" and not table.append and table != DEFAULT_EMPTY_TABLE:
            if get_config(client)["yamr_mode"]["abort_transactions_with_remove"]:
                _remove_locks(table, client=client)
            remove(table, client=client)

def _add_user_command_spec(op_type, binary, format, input_format, output_format,
                           files, file_paths, local_files, yt_files,
                           memory_limit, reduce_by, spec, client=None):
    if binary is None:
        return spec, []

    if local_files is not None:
        require(files is None, YtError("You cannot specify files and local_files simultaneously"))
        files = local_files

    if yt_files is not None:
        require(file_paths is None, YtError("You cannot specify yt_files and file_paths simultaneously"))
        file_paths = yt_files

    files = _reliably_upload_files(files, client=client)
    input_format, output_format = _prepare_formats(format, input_format, output_format, binary=binary, client=client)
    binary, additional_files = _prepare_binary(binary, op_type, input_format, output_format,
                                               reduce_by, client=client)
    spec = update(
        {
            op_type: {
                "input_format": input_format.to_yson_type(),
                "output_format": output_format.to_yson_type(),
                "command": binary,
                "file_paths":
                    flatten(files + additional_files + map(lambda path: prepare_path(path, client=client), get_value(file_paths, []))),
                "use_yamr_descriptors": bool_to_string(get_config(client)["yamr_mode"]["use_yamr_style_destination_fds"]),
                "check_input_fully_consumed": bool_to_string(get_config(client)["yamr_mode"]["check_input_fully_consumed"])
            }
        },
        spec)

    # NB: Configured by common rule now.
    memory_limit = get_value(memory_limit, get_config(client)["memory_limit"])
    if memory_limit is not None:
        spec = update({op_type: {"memory_limit": int(memory_limit)}}, spec)
    return spec, files + additional_files

def _configure_spec(spec, client):
    started_by = {
        "hostname": socket.getfqdn(),
        "pid": os.getpid(),
        "user": getpass.getuser(),
        "command": sys.argv,
        "wrapper_version": get_version()}
    spec = update({"started_by": started_by}, spec)
    spec = update(deepcopy(get_config(client)["spec_defaults"]), spec)
    if client is None and config.SPEC is not None:
        spec = update(json.loads(get_option("SPEC", client)), spec)
    return spec

def _add_input_output_spec(source_table, destination_table, spec):
    def get_input_name(table):
        return table.to_yson_type()
    def get_output_name(table):
        return table.to_yson_type()

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

def _make_operation_request(command_name, spec, strategy, sync,
                            finalizer=None, verbose=False, client=None):
    def _manage_operation(finalizer):
        operation_id = _make_formatted_transactional_request(command_name, {"spec": spec}, format=None,
                                                             verbose=verbose, client=client)
        if strategy is not None:
            get_value(strategy, WaitStrategy()).process_operation(command_name, operation_id,
                                                                           finalizer, client=client)
        else:
            operation = Operation(command_name, operation_id, finalizer, client=client)
            if sync:
                operation.wait()
            return operation


    if get_config(client)["detached"]:
        return _manage_operation(finalizer)
    else:
        transaction = Transaction(
            attributes={"title": "Python wrapper: envelope transaction of operation"},
            client=client)

        def finish_transaction():
            transaction.__exit__(*sys.exc_info())

        def attached_mode_finalizer(state):
            transaction.__exit__(None, None, None)
            if finalizer is not None:
                finalizer(state)

        transaction.__enter__()
        with KeyboardInterruptsCatcher(finish_transaction, enable=get_config(client)["operation_tracker"]["abort_on_sigint"]):
            return _manage_operation(attached_mode_finalizer)

def _get_format_from_tables(tables, ignore_unexisting_tables):
    """Try to get format from tables, raise YtError if tables have different _format attribute"""
    not_none_tables = filter(None, flatten(tables))

    if ignore_unexisting_tables:
        tables_to_extract = filter(lambda x: exists(to_name(x)), not_none_tables)
    else:
        tables_to_extract = not_none_tables

    if not tables_to_extract:
        return None

    def extract_format(table):
        table_name = to_table(table).name

        if not exists(table_name):
            return None

        if has_attribute(table_name, "_format"):
            format_name = get(table_name + "/@_format", format=YsonFormat())
            return create_format(format_name)
        return None
    formats = map(extract_format, tables_to_extract)

    require(len(set(repr(format) for format in formats)) == 1,
            YtError("Tables have different attribute _format: " + repr(formats)))

    return formats[0]

""" Common table methods """

def create_table(path, recursive=None, ignore_existing=False,
                 replication_factor=None, compression_codec=None, attributes=None, client=None):
    """Create empty table.

    Shortcut for `create("table", ...)`.
    :param path: (string or :py:class:`yt.wrapper.table.TablePath`) path to table
    :param recursive: (bool) create the path automatically, `config["yamr_mode"]["create_recursive"]` by default
    :param ignore_existing: (bool) if it sets to `False` and table exists, \
                            Python Wrapper raises `YtResponseError`.
    :param replication_factor: (int) number of data replicas
    :param attributes: (dict)
    """
    table = to_table(path, client=client)
    attributes = get_value(attributes, {})
    if replication_factor is not None:
        attributes["replication_factor"] = replication_factor
    if compression_codec is not None:
        attributes["compression_codec"] = compression_codec
    create("table", table.name, recursive=recursive, ignore_existing=ignore_existing,
           attributes=attributes, client=client)

def create_temp_table(path=None, prefix=None, client=None):
    """Create temporary table by given path with given prefix and return name.

    :param path: (string or :py:class:`yt.wrapper.table.TablePath`) existing path, \
                 by default `config["remote_temp_tables_directory"]`
    :param prefix: (string) prefix of table name
    :return: (string) name of result table
    """
    if path is None:
        path = get_config(client)["remote_temp_tables_directory"]
        mkdir(path, recursive=True, client=client)
    else:
        path = to_name(path, client=client)
    require(exists(path, client=client), YtError("You cannot create table in unexisting path"))
    if prefix is not None:
        path = os.path.join(path, prefix)
    else:
        if not path.endswith("/"):
            path = path + "/"
    name = find_free_subpath(path, client=client)
    create_table(name, client=client)
    return name

def write_table(table, input_stream, format=None, table_writer=None,
                replication_factor=None, compression_codec=None, client=None, raw=True):
    """Write rows from input_stream to table.

    :param table: (string or :py:class:`yt.wrapper.table.TablePath`) output table. Specify \
                `TablePath` attributes for append mode or something like this. Table can not exist.
    :param input_stream: python file-like object, string, list of strings, `StringIterIO`.
    :param format: (string or subclass of `Format`) format of input data, \
                    `yt.wrapper.config["tabular_data_format"]` by default.
    :param table_writer: (dict) spec of "write" operation
    :param replication_factor: (integer) number of data replicas
    :param compression_codec: (string) standard operation parameter

    Python Wrapper try to split input stream to portions of fixed size and write its with retries.
    If splitting fails, stream is written as is through HTTP.
    Set `yt.wrapper.config["write_retries"]["enable"]` to ``False`` for writing \
    without splitting and retries.

    Writing is executed under self-pinged transaction.
    """
    table = to_table(table, client=client)
    format = _prepare_format(format, raw, client)

    params = {}
    params["input_format"] = format.to_yson_type()
    if table_writer is not None:
        params["table_writer"] = table_writer

    def prepare_table(path):
        if exists(path, client=client):
            require(replication_factor is None and compression_codec is None,
                    YtError("Cannot write to existing path %s "
                            "with set replication factor or compression codec" % path))
        else:
            create_table(path, ignore_existing=True, replication_factor=replication_factor,
                         compression_codec=compression_codec, client=client)

    can_split_input = isinstance(input_stream, types.ListType) or format.is_raw_load_supported()
    enable_retries = get_config(client)["write_retries"]["enable"] and can_split_input and "sorted_by" not in table.attributes
    if enable_retries:
        input_stream = chunk_iter_lines(_split_rows(input_stream, format, raw), get_config(client)["write_retries"]["chunk_size"])
    elif isinstance(input_stream, file) or hasattr(input_stream, "read"):
        input_stream = chunk_iter_stream(input_stream, MB)

    if get_config(client)["write_retries"]["enable"] and not can_split_input:
        logger.warning("Cannot split input into rows. Write is processing by one request.")

    make_write_request(
        "write" if get_api_version(client=client) == "v2" else "write_table",
        input_stream,
        table,
        params,
        prepare_table,
        use_retries=enable_retries,
        client=client)

    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and is_empty(table, client=client):
        _remove_tables([table], client=client)

def read_table(table, format=None, table_reader=None, response_type=None, raw=True, response_parameters=None, read_transaction=None, client=None):
    """Read rows from table and parse (optionally).

    :param table: string or :py:class:`yt.wrapper.table.TablePath`
    :param table_reader: (dict) spec of "read" operation
    :param response_type: output type, line generator by default. ["iter_lines", "iter_content", \
                          "raw", "string"]
    :param raw: (bool) don't parse response to rows
    :return: if `raw` is specified -- string or :class:`yt.wrapper.driver.ResponseStream`,\
             else -- rows generator (python dict or :class:`yt.wrapper.yamr_record.Record`)

    If :py:data:`yt.wrapper.config["read_retries"]["enable"]` is specified,
    command is executed under self-pinged transaction with retries and snapshot lock on the table.
    """
    if response_type is not None:
        logger.info("Option response_type is deprecated and ignored")

    table = to_table(table, client=client)
    format = _prepare_format(format, raw, client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not exists(table.name, client=client):
        return StringIO() if raw else EMPTY_GENERATOR
    attributes = get(table.name + "/@", client=client)
    if  attributes["chunk_count"] > 100 and attributes["compressed_data_size"] / attributes["chunk_count"] < MB:
        logger.info("Table chunks are too small; consider running the following command to improve read performance: "
                    "yt merge --proxy {1} --src {0} --dst {0} "
                    "--spec '{{"
                       "combine_chunks=true;"
                    "}}'".format(table.name, get_config(client)["proxy"]["url"]))

    params = {
        "path": table.to_yson_type(),
        "output_format": format.to_yson_type()
    }
    if table_reader is not None:
        params["table_reader"] = table_reader

    command_name = "read" if get_api_version(client=client) == "v2" else "read_table"

    def set_response_parameters(parameters):
        if response_parameters is not None:
            for key in parameters:
                response_parameters[key] = parameters[key]

    def process_response(response):
        if response.response_parameters is None:
            raise YtIncorrectResponse("X-YT-Response-Parameters missing (bug in proxy)", response)
        set_response_parameters(response.response_parameters)

    class RetriableState(object):
        def __init__(self):
            self.started = False
            self.index = None

        def prepare_params_for_retry(self):
            if not self.started:
                return params

            if "ranges" not in table.name.attributes:
                table.name.attributes["lower_limit"] = {"row_index": self.index}
            else:
                if len(table.name.attributes["ranges"]) > 1:
                    raise YtError("Read table with multiple tanges using retries is not supported")
                table.name.attributes["ranges"][0]["lower_limit"] = {"row_index": self.index}
            params["path"] = table.to_yson_type()
            return params

        def iterate(self, response):
            if not self.started:
                process_response(response)
                self.index = response.response_parameters.get("start_row_index", None)
                self.started = True
            for row in format.load_rows(response, raw=True):
                yield row
                self.index += 1

    # For read commands response is actually ResponseStream
    response = make_read_request(
        command_name,
        table,
        params,
        process_response_action=process_response,
        retriable_state_class=RetriableState,
        client=client)

    if raw:
        return response
    else:
        return format.load_rows(response)

def _are_nodes(source_tables, destination_table):
    return len(source_tables) == 1 and \
           not source_tables[0].has_delimiters() and \
           not destination_table.append

def copy_table(source_table, destination_table, replace=True, client=None):
    """Copy table(s).

    :param source_table: string, `TablePath` or list of them
    :param destination_table: string or `TablePath`
    :param replace: (bool) override `destination_table`

    .. note:: param `replace` is overridden by set \
              `yt.wrapper.config["yamr_mode"]["replace_tables_on_copy_and_move"]`
    If `source_table` is a list of tables, tables would be merged.
    """
    if get_config(client)["yamr_mode"]["replace_tables_on_copy_and_move"]:
        replace = True
    source_tables = _prepare_source_tables(source_table, client=client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and _are_default_empty_table(source_tables):
        return
    destination_table = to_table(destination_table, client=client)
    if _are_nodes(source_tables, destination_table):
        if replace and exists(destination_table.name, client=client) and \
           to_name(source_tables[0], client=client) != to_name(destination_table, client=client):
            # in copy destination should be missing
            remove(destination_table.name, client=client)
        dirname = os.path.dirname(destination_table.name)
        if dirname != "//":
            mkdir(dirname, recursive=True, client=client)
        copy(source_tables[0].name, destination_table.name, client=client)
    else:
        source_names = [table.name for table in source_tables]
        mode = "sorted" if (all(map(lambda t: is_sorted(t, client=client), source_names)) and not destination_table.append) \
               else "ordered"
        run_merge(source_tables, destination_table, mode, client=client)

def move_table(source_table, destination_table, replace=True, client=None):
    """Move table.

    :param source_table: string, `TablePath` or list of them
    :param destination_table: string or `TablePath`
    :param replace: (bool) override `destination_table`

    .. note:: param `replace` is overridden by `yt.wrapper.config["yamr_mode"]["replace_tables_on_copy_and_move"]`

    If `source_table` is a list of tables, tables would be merged.
    """
    if get_config(client)["yamr_mode"]["replace_tables_on_copy_and_move"]:
        replace = True
    source_tables = _prepare_source_tables(source_table, client=client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and _are_default_empty_table(source_tables):
        remove(to_table(destination_table).name, client=client, force=True)
        return
    destination_table = to_table(destination_table, client=client)
    if _are_nodes(source_tables, destination_table):
        if source_tables[0] == destination_table:
            return
        if replace and exists(destination_table.name, client=client):
            remove(destination_table.name, client=client)
        dirname = os.path.dirname(destination_table.name)
        if dirname != "//":
            mkdir(dirname, recursive=True, client=client)
        move(source_tables[0].name, destination_table.name, client=client)
    else:
        copy_table(source_table, destination_table, client=client)
        for table in source_tables:
            if to_name(table, client=client) == to_name(destination_table, client=client):
                continue
            if table == DEFAULT_EMPTY_TABLE:
                continue
            remove(table.name, client=client, force=True)


def records_count(table, client=None):
    """Return number of records in the table.

    :param table: string or `TablePath`
    :return: integer
    """
    table = to_name(table, client=client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not exists(table, client=client):
        return 0
    return get_attribute(table, "row_count", client=client)

def is_empty(table, client=None):
    """Is table empty?

    :param table: (string or `TablePath`)
    :return: (bool)
    """
    return records_count(to_name(table, client=client), client=client) == 0

def get_sorted_by(table, default=None, client=None):
    """Get 'sorted_by' table attribute or `default` if attribute doesn't exist.

    :param table: string or `TablePath`
    :param default: whatever
    :return: string or list of string
    """
    if default is None:
        default = [] if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] else None
    return get_attribute(to_name(table, client=client), "sorted_by", default=default, client=client)

def is_sorted(table, client=None):
    """Is table sorted?

    :param table: string or `TablePath`
    :return: bool
    """
    if get_config(client)["yamr_mode"]["use_yamr_sort_reduce_columns"]:
        return get_sorted_by(table, [], client=client) == ["key", "subkey"]
    else:
        return parse_bool(
            get_attribute(to_name(table, client=client),
                          "sorted",
                          default="false",
                          client=client))

def mount_table(path, first_tablet_index=None, last_tablet_index=None, cell_id=None, client=None):
    """Mount table (or a part of it).  NB! This command is not currently supported! The feature is coming with 0.17+ version!

    description is coming with tablets
    TODO
    """
    # TODO(ignat): Add path preparetion
    params = {"path": path}
    if first_tablet_index is not None:
        params["first_tablet_index"] = first_tablet_index
    if last_tablet_index is not None:
        params["last_tablet_index"] = last_tablet_index
    if cell_id is not None:
        params["cell_id"] = cell_id

    make_request("mount_table", params, client=client)

def unmount_table(path, first_tablet_index=None, last_tablet_index=None, force=None, client=None):
    """Unmount table (or a part of it).  NB! This command is not currently supported! The feature is coming with 0.17+ version!

    description is coming with tablets
    TODO
    """
    params = {"path": path}
    if first_tablet_index is not None:
        params["first_tablet_index"] = first_tablet_index
    if last_tablet_index is not None:
        params["last_tablet_index"] = last_tablet_index
    if force is not None:
        params["force"] = bool_to_string(force)

    make_request("unmount_table", params, client=client)

def remount_table(path, first_tablet_index=None, last_tablet_index=None, client=None):
    """Remount table (or a part of it).  NB! This command is not currently supported! The feature is coming with 0.17+ version!

    description is coming with tablets
    TODO
    """
    params = {"path": path}
    if first_tablet_index is not None:
        params["first_tablet_index"] = first_tablet_index
    if last_tablet_index is not None:
        params["last_tablet_index"] = last_tablet_index

    make_request("remount_table", params, client=client)

def reshard_table(path, pivot_keys, first_tablet_index=None, last_tablet_index=None, client=None):
    """Change pivot keys separating tablets of a given table.  NB! This command is not currently supported! The feature is coming with 0.17+ version!

    description is coming with tablets
    TODO
    """
    params = {"path": path,
              "pivot_keys": pivot_keys}
    if first_tablet_index is not None:
        params["first_tablet_index"] = first_tablet_index
    if last_tablet_index is not None:
        params["last_tablet_index"] = last_tablet_index

    make_request("reshard_table", params, client=client)

def select_rows(query, timestamp=None, input_row_limit=None, output_row_limit=None, verbose_logging=None, enable_code_cache=None, format=None, raw=True, client=None):
    """Execute a SQL-like query. NB! This command is not currently supported! The feature is coming with 0.17+ version!

    .. seealso:: `supported features <https://wiki.yandex-team.ru/yt/userdoc/queries>`_

    :param query: (string) for example \"<columns> [as <alias>], ... from \[<table>\] \
                  [where <predicate> [group by <columns> [as <alias>], ...]]\"
    :param timestamp: (string) TODO(veronikaiv): verify
    :param format: (string or descendant of `Format`) output format
    :param raw: (bool) don't parse response to rows
    """
    format = _prepare_format(format, raw, client)
    params = {
        "query": query,
        "output_format": format.to_yson_type()}
    if timestamp is not None:
        params["timestamp"] = timestamp
    if input_row_limit is not None:
        params["input_row_limit"] = input_row_limit
    if output_row_limit is not None:
        params["output_row_limit"] = output_row_limit
    if verbose_logging is not None:
        params["verbose_logging"] = bool_to_string(verbose_logging)
    if enable_code_cache is not None:
        params["enable_code_cache"] = bool_to_string(enable_code_cache)

    response = _make_transactional_request(
        "select_rows",
        params,
        return_content=False,
        use_heavy_proxy=True,
        client=client)

    if raw:
        return response
    else:
        return format.load_rows(response)

def lookup_rows(table, input_stream, format=None, raw=False, client=None):
    """Lookup rows in dynamic table. NB! This command is not currently supported! The feature is coming with 0.17+ version!

    .. seealso:: `supported features <https://wiki.yandex-team.ru/yt/userdoc/queries>`_

    :param format: (string or descendant of `Format`) output format
    :param raw: (bool) don't parse response to rows
    """

    table = to_table(table, client=client)
    format = _prepare_format(format, raw, client)

    params = {}
    params["path"] = table.to_yson_type()
    params["input_format"] = format.to_yson_type()
    params["output_format"] = format.to_yson_type()

    if isinstance(input_stream, types.ListType) or format.is_raw_load_supported():
        input_stream = _split_rows(input_stream, format, raw)
    elif isinstance(input_stream, file) or hasattr(input_stream, "read"):
        input_stream = chunk_iter_stream(input_stream, MB)

    response = _make_transactional_request(
        "lookup_rows",
        params,
        data=input_stream,
        return_content=False,
        use_heavy_proxy=True,
        client=client)

    if raw:
        return response
    else:
        return format.load_rows(response)

def insert_rows(table, input_stream, format=None, raw=False, client=None):
    """Write rows from input_stream to table.

    :param table: (string or :py:class:`yt.wrapper.table.TablePath`) output table. Specify \
                `TablePath` attributes for append mode or something like this. Table can not exist.
    :param input_stream: python file-like object, string, list of strings, `StringIterIO`.
    :param format: (string or subclass of `Format`) format of input data, \
                    `yt.wrapper.config["tabular_data_format"]` by default.

    """
    table = to_table(table, client=client)
    format = _prepare_format(format, raw, client)

    params = {}
    params["path"] = table.to_yson_type()
    params["input_format"] = format.to_yson_type()

    if isinstance(input_stream, types.ListType) or format.is_raw_load_supported():
        input_stream = _split_rows(input_stream, format, raw)
    elif isinstance(input_stream, file) or hasattr(input_stream, "read"):
        input_stream = chunk_iter_stream(input_stream, MB)

    _make_transactional_request(
        "insert_rows",
        params,
        data=input_stream,
        use_heavy_proxy=True,
        client=client)

def delete_rows(table, input_stream, format=None, raw=False, client=None):
    """Write rows from input_stream to table.

    :param table: (string or :py:class:`yt.wrapper.table.TablePath`) output table. Specify \
                `TablePath` attributes for append mode or something like this. Table can not exist.
    :param input_stream: python file-like object, string, list of strings, `StringIterIO`.
    :param format: (string or subclass of `Format`) format of input data, \
                    `yt.wrapper.config["tabular_data_format"]` by default.

    """
    table = to_table(table, client=client)
    format = _prepare_format(format, raw, client)

    params = {}
    params["path"] = table.to_yson_type()
    params["input_format"] = format.to_yson_type()

    if isinstance(input_stream, types.ListType) or format.is_raw_load_supported():
        input_stream = _split_rows(input_stream, format, raw)
    elif isinstance(input_stream, file) or hasattr(input_stream, "read"):
        input_stream = chunk_iter_stream(input_stream, MB)

    _make_transactional_request(
        "delete_rows",
        params,
        data=input_stream,
        use_heavy_proxy=True,
        client=client)

# Operations.

def run_erase(table, spec=None, strategy=None, sync=True, client=None):
    """Erase table or part of it.

    Erase differs from remove command.
    It only removes content of table (range of records or all table) and doesn't remove Cypress node.

    :param table: (string or `TablePath`)
    :param spec: (dict)
    :param strategy: standard operation parameter

    .. seealso::  :ref:`operation_parameters`.
    """
    table = to_table(table, client=client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not exists(table.name, client=client):
        return
    spec = update({"table_path": table.to_yson_type()}, get_value(spec, {}))
    spec = _configure_spec(spec, client)
    return _make_operation_request("erase", spec, strategy, sync, client=client)

def run_merge(source_table, destination_table, mode=None,
              strategy=None, sync=True, table_writer=None,
              replication_factor=None, compression_codec=None,
              job_count=None, spec=None, client=None):
    """Merge source tables to destination table.

    :param source_table: list of string or `TablePath`, list tables names to merge
    :param destination_table: string or `TablePath`, path to result table
    :param mode: ['auto' (default), 'unordered', 'ordered', or 'sorted']. Mode `sorted` keeps sortedness \
                 of output tables, mode `ordered` is about chunk magic, not for ordinary users.
                 In 'auto' mode system chooses proper mode depending on the table sortedness.
    :param strategy: standard operation parameter
    :param table_writer: standard operation parameter
    :param replication_factor: (int) number of destination table replicas.
    :param compression_codec: (string) compression algorithm of destination_table.
    :param job_count: (integer) standard operation parameter.
    :param spec: (dict) standard operation parameter.


    .. seealso::  :ref:`operation_parameters`.
    """
    source_table = _prepare_source_tables(source_table, replace_unexisting_by_empty=False, client=client)
    destination_table = unlist(_prepare_destination_tables(destination_table, replication_factor,
                                                           compression_codec, client=client))

    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not source_table:
        _remove_tables([destination_table], client=client)
        return

    mode = get_value(mode, "auto")
    if mode == "auto":
        mode = "sorted" if all(map(lambda t: is_sorted(t, client=client), source_table)) else "unordered"

    spec = compose(
        lambda _: _configure_spec(_, client),
        lambda _: _add_table_writer_spec("job_io", table_writer, _),
        lambda _: _add_input_output_spec(source_table, destination_table, _),
        lambda _: update({"job_count": job_count}, _) if job_count is not None else _,
        lambda _: update({"mode": mode}, _),
        lambda _: get_value(_, {})
    )(spec)

    return _make_operation_request("merge", spec, strategy, sync, finalizer=None, client=client)

def run_sort(source_table, destination_table=None, sort_by=None,
             strategy=None, sync=True, table_writer=None, replication_factor=None,
             compression_codec=None, spec=None, client=None):
    """Sort source tables to destination table.

    If destination table is not specified, than it equals to source table.

    .. seealso::  :ref:`operation_parameters`.
    """

    sort_by = _prepare_sort_by(sort_by, client)
    source_table = _prepare_source_tables(source_table, replace_unexisting_by_empty=False, client=client)
    for table in source_table:
        require(exists(table.name, client=client), YtError("Table %s should exist" % table))

    if destination_table is None:
        if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not source_table:
            return
        require(len(source_table) == 1 and not source_table[0].has_delimiters(),
                YtError("You must specify destination sort table "
                        "in case of multiple source tables"))
        destination_table = source_table[0]
    destination_table = unlist(_prepare_destination_tables(destination_table, replication_factor,
                                                           compression_codec, client=client))

    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not source_table:
        _remove_tables([destination_table], client=client)
        return

    if all(sort_by == get_sorted_by(table.name, [], client=client) for table in source_table):
        run_merge(source_table, destination_table, "sorted",
                  strategy=strategy, table_writer=table_writer, spec=spec, client=client)
        return

    spec = compose(
        lambda _: _configure_spec(_, client),
        lambda _: _add_table_writer_spec(["sort_job_io", "merge_job_io"], table_writer, _),
        lambda _: _add_input_output_spec(source_table, destination_table, _),
        lambda _: update({"sort_by": sort_by}, _),
        lambda _: get_value(_, {})
    )(spec)

    return _make_operation_request("sort", spec, strategy, sync, finalizer=None, client=client)

class Finalizer(object):
    """Entity for operation finalizing: checking size of result chunks, deleting of \
    empty output tables and uploaded files.
    """
    def __init__(self, files, output_tables, client=None):
        self.files = files if files is not None else []
        self.output_tables = output_tables
        self.client = client

    def __call__(self, state):
        if state == "completed":
            for table in map(lambda table: to_name(table, client=self.client), self.output_tables):
                self.check_for_merge(table)
        if get_config(self.client)["yamr_mode"]["delete_empty_tables"]:
            for table in map(lambda table: to_name(table, client=self.client), self.output_tables):
                if is_empty(table, client=self.client):
                    remove_with_empty_dirs(table, client=self.client)

    def check_for_merge(self, table):
        if get_config(self.client)["auto_merge_output"]["action"] == "none":
            return

        chunk_count = int(get_attribute(table, "chunk_count", client=self.client))
        if  chunk_count < get_config(self.client)["auto_merge_output"]["min_chunk_count"]:
            return

        # We use uncompressed data size to simplify recommended command
        chunk_size = float(get_attribute(table, "compressed_data_size", client=self.client)) / chunk_count
        if chunk_size > get_config(self.client)["auto_merge_output"]["max_chunk_size"]:
            return

        compression_ratio = get_attribute(table, "compression_ratio", client=self.client)
        data_size_per_job = min(16 * 1024 * MB, int(500 * MB / float(compression_ratio)))

        mode = "sorted" if is_sorted(table, client=self.client) else "unordered"

        if get_config(self.client)["auto_merge_output"]["action"] == "merge":
            table = TablePath(table, append=False)
            try:
                run_merge(source_table=table, destination_table=table, mode=mode,
                          spec={"combine_chunks": bool_to_string(True), "data_size_per_job": data_size_per_job},
                          client=self.client)
            except YtOperationFailedError():
                logger.warning("Failed to merge table " + table)
        else:
            logger.info("Chunks of output table {0} are too small. "
                        "This may cause suboptimal system performance. "
                        "If this table is not temporary then consider running the following command:\n"
                        "yt merge --mode {1} --proxy {3} --src {0} --dst {0} "
                        "--spec '{{"
                           "combine_chunks=true;"
                           "data_size_per_job={2}"
                        "}}'".format(table, mode, data_size_per_job, get_config(self.client)["proxy"]["url"]))


def run_map_reduce(mapper, reducer, source_table, destination_table,
                   format=None,
                   map_input_format=None, map_output_format=None,
                   reduce_input_format=None, reduce_output_format=None,
                   strategy=None, sync=True, table_writer=None, spec=None,
                   replication_factor=None, compression_codec=None,
                   map_files=None, map_file_paths=None,
                   map_local_files=None, map_yt_files=None,
                   reduce_files=None, reduce_file_paths=None,
                   reduce_local_files=None, reduce_yt_files=None,
                   mapper_memory_limit=None, reducer_memory_limit=None,
                   sort_by=None, reduce_by=None,
                   reduce_combiner=None,
                   reduce_combiner_input_format=None, reduce_combiner_output_format=None,
                   reduce_combiner_files=None, reduce_combiner_file_paths=None,
                   reduce_combiner_local_files=None, reduce_combiner_yt_files=None,
                   reduce_combiner_memory_limit=None,
                   client=None):
    """Run map (optionally), sort, reduce and reduce-combine (optionally) operations.

    :param mapper: (python generator, callable object-generator or string (with bash commands)).
    :param reducer: (python generator, callable object-generator or string (with bash commands)).
    :param source_table: (string, `TablePath` or list of them) input tables
    :param destination_table: (string, `TablePath` or list of them) output tables
    :param format: (string or descendant of `yt.wrapper.format.Format`) common format of input, \
                    intermediate and output data. More specific formats will override it.
    :param map_input_format: (string or descendant of `yt.wrapper.format.Format`)
    :param map_output_format: (string or descendant of `yt.wrapper.format.Format`)
    :param reduce_input_format: (string or descendant of `yt.wrapper.format.Format`)
    :param reduce_output_format: (string or descendant of `yt.wrapper.format.Format`)
    :param strategy:  standard operation parameter
    :param table_writer: (dict) standard operation parameter
    :param spec: (dict) standard operation parameter
    :param replication_factor: (int) standard operation parameter
    :param compression_codec: standard operation parameter
    :param map_files: Deprecated!
    :param map_file_paths: Deprecated!
    :param map_local_files: (string or list  of string) paths to map scripts on local machine.
    :param map_yt_files: (string or list  of string) paths to map scripts in Cypress.
    :param reduce_files: Deprecated!
    :param reduce_file_paths: Deprecated!
    :param reduce_local_files: (string or list  of string) paths to reduce scripts on local machine.
    :param reduce_yt_files: (string or list of string) paths to reduce scripts in Cypress.
    :param mapper_memory_limit: (integer) in bytes, map **job** memory limit.
    :param reducer_memory_limit: (integer) in bytes, reduce **job** memory limit.
    :param sort_by: (list of strings, string) list of columns for sorting by, \
                    equals to `reduce_by` by default
    :param reduce_by: (list of strings, string) list of columns for grouping by
    :param reduce_combiner: (python generator, callable object-generator or string \
                            (with bash commands)).
    :param reduce_combiner_input_format: (string or descendant of `yt.wrapper.format.Format`)
    :param reduce_combiner_output_format: (string or descendant of `yt.wrapper.format.Format`)
    :param reduce_combiner_files: Deprecated!
    :param reduce_combiner_file_paths: Deprecated!
    :param reduce_combiner_local_files: (string or list  of string) \
                                        paths to reduce combiner scripts on local machine.
    :param reduce_combiner_yt_files: (string or list  of string) \
                                     paths to reduce combiner scripts in Cypress.
    :param reduce_combiner_memory_limit: (integer) in bytes


    .. seealso::  :ref:`operation_parameters`.
    """

    run_map_reduce.files_to_remove = []
    def memorize_files(spec, files):
        run_map_reduce.files_to_remove += files
        return spec

    source_table = _prepare_source_tables(source_table, client=client)
    destination_table = _prepare_destination_tables(destination_table, replication_factor,
                                                    compression_codec, client=client)

    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and _are_default_empty_table(source_table):
        _remove_tables(destination_table, client=client)
        return

    if sort_by is None:
        sort_by = reduce_by

    reduce_by = _prepare_reduce_by(reduce_by, client)
    sort_by = _prepare_sort_by(sort_by, client)

    spec = compose(
        lambda _: _configure_spec(_, client),
        lambda _: _add_table_writer_spec(["map_job_io", "reduce_job_io", "sort_job_io"],
                                         table_writer, _),
        lambda _: _add_input_output_spec(source_table, destination_table, _),
        lambda _: update({"sort_by": sort_by, "reduce_by": reduce_by}, _),
        lambda _: memorize_files(*_add_user_command_spec("mapper", mapper,
            format, map_input_format, map_output_format,
            map_files, map_file_paths,
            map_local_files, map_yt_files,
            mapper_memory_limit, None, _, client=client)),
        lambda _: memorize_files(*_add_user_command_spec("reducer", reducer,
            format, reduce_input_format, reduce_output_format,
            reduce_files, reduce_file_paths,
            reduce_local_files, reduce_yt_files,
            reducer_memory_limit, reduce_by, _, client=client)),
        lambda _: memorize_files(*_add_user_command_spec("reduce_combiner", reduce_combiner,
            format, reduce_combiner_input_format, reduce_combiner_output_format,
            reduce_combiner_files, reduce_combiner_file_paths,
            reduce_combiner_local_files, reduce_combiner_yt_files,
            reduce_combiner_memory_limit, reduce_by, _, client=client)),
        lambda _: get_value(_, {})
    )(spec)

    return _make_operation_request("map_reduce", spec, strategy, sync,
                                   finalizer=Finalizer(run_map_reduce.files_to_remove,
                                                       destination_table, client=client),
                                   client=client)

def _run_operation(binary, source_table, destination_table,
                  files=None, file_paths=None,
                  local_files=None, yt_files=None,
                  format=None, input_format=None, output_format=None,
                  strategy=None, sync=True,
                  table_writer=None,
                  replication_factor=None,
                  compression_codec=None,
                  job_count=None,
                  memory_limit=None,
                  spec=None,
                  op_name=None,
                  sort_by=None,
                  reduce_by=None,
                  client=None):
    """Run script operation.

    :param binary: (python generator, callable object-generator or string (with bash commands))
    :param files: Deprecated!
    :param file_paths: Deprecated!
    :param local_files: (string or list  of string) paths to scripts on local machine.
    :param yt_files: (string or list  of string) paths to scripts in Cypress.
    :param op_name: (one of "map" (default), "reduce", ...) TODO(veronikaiv): list it!

    .. seealso::  :ref:`operation_parameters` and :py:func:`yt.wrapper.table_commands.run_map_reduce`.
    """
    _run_operation.files = []
    def memorize_files(spec, files):
        _run_operation.files += files
        return spec
    op_name = get_value(op_name, "map")
    source_table = _prepare_source_tables(source_table, client=client)
    destination_table = _prepare_destination_tables(destination_table, replication_factor,
                                                    compression_codec, client=client)

    are_sorted_output = False
    for table in destination_table:
        if table.attributes.get("sorted_by") is not None:
            are_sorted_output = True

    finalize = None

    if op_name == "reduce":
        if sort_by is None:
            sort_by = _prepare_sort_by(reduce_by, client)
        else:
            sort_by = _prepare_sort_by(sort_by, client)
        reduce_by = _prepare_reduce_by(reduce_by, client)

        if get_config(client)["yamr_mode"]["run_map_reduce_if_source_is_not_sorted"]:
            are_tables_not_sorted = False
            for table in source_table:
                sorted_by = get_sorted_by(table.name, [], client=client)
                if not sorted_by:
                    are_tables_not_sorted = True
                    continue
                if not is_prefix(reduce_by, sorted_by):
                    raise YtError("reduce_by parameter {0} conflicts with sorted_by attribute {1} of input table {2}".format(reduce_by, sorted_by, table.name))

            if are_tables_not_sorted and not are_sorted_output:
                if job_count is not None:
                    spec = update({"partition_count": job_count}, spec)
                run_map_reduce(
                    mapper=None,
                    reducer=binary,
                    reduce_files=files,
                    reduce_local_files=local_files,
                    reduce_file_paths=file_paths,
                    reduce_yt_files=yt_files,
                    source_table=source_table,
                    destination_table=destination_table,
                    format=format,
                    reduce_input_format=input_format,
                    reduce_output_format=output_format,
                    table_writer=table_writer,
                    reduce_by=reduce_by,
                    sort_by=sort_by,
                    replication_factor=replication_factor,
                    compression_codec=compression_codec,
                    reducer_memory_limit=memory_limit,
                    strategy=strategy,
                    spec=spec)
                return

            if are_sorted_output and are_tables_not_sorted:
                logger.info("Sorting %s", source_table)
                temp_table = create_temp_table(client=client)
                run_sort(source_table, temp_table, sort_by=sort_by, client=client)
                finalize = lambda: remove(temp_table, client=client)
                source_table = [TablePath(temp_table)]


    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and _are_default_empty_table(source_table):
        _remove_tables(destination_table, client=client)
        return

    op_type = None
    if op_name == "map": op_type = "mapper"
    if op_name == "reduce": op_type = "reducer"

    try:
        spec = compose(
            lambda _: _configure_spec(_, client),
            lambda _: _add_table_writer_spec("job_io", table_writer, _),
            lambda _: _add_input_output_spec(source_table, destination_table, _),
            lambda _: update({"reduce_by": reduce_by}, _) if op_name == "reduce" else _,
            lambda _: update({"job_count": job_count}, _) if job_count is not None else _,
            lambda _: memorize_files(*_add_user_command_spec(op_type, binary,
                format, input_format, output_format,
                files, file_paths,
                local_files, yt_files,
                memory_limit, reduce_by, _, client=client)),
            lambda _: get_value(_, {})
        )(spec)

        return _make_operation_request(op_name, spec, strategy, sync,
                                       finalizer=Finalizer(_run_operation.files, destination_table, client=client),
                                       client=client)
    finally:
        if finalize is not None:
            finalize()

def run_map(binary, source_table, destination_table, **kwargs):
    """Run map operation.
    .. seealso::  :ref:`operation_parameters` and :py:func:`yt.wrapper.table_commands.run_map_reduce`.
    """
    kwargs["op_name"] = "map"
    return _run_operation(binary, source_table, destination_table, **kwargs)

def run_reduce(binary, source_table, destination_table, **kwargs):
    """Run reduce operation.
    .. seealso::  :ref:`operation_parameters` and :py:func:`yt.wrapper.table_commands.run_map_reduce`.
    """
    kwargs["op_name"] = "reduce"
    return _run_operation(binary, source_table, destination_table, **kwargs)

def run_remote_copy(source_table, destination_table, cluster_name,
                    network_name=None, spec=None, copy_attributes=False, remote_cluster_token=None, strategy=None, sync=True, client=None):
    """Copy source table from remote cluster to destination table on current cluster.

    :param source_table: (list of string or `TablePath`)
    :param destination_table: (string, `TablePath`)
    :param cluster_name: (string)
    :param network_name: (string)
    :param spec: (dict)
    :param copy_attributes: (bool) copy attributes source_table to destination_table
    :param strategy: standard operation parameter

    .. note:: For atomicity you should specify just one item in `source_table` \
    in case attributes copying.

    .. seealso::  :ref:`operation_parameters`.
    """

    # TODO(ignat): implement client-version of remote copy, that supports native driver

    def get_input_name(table):
        return to_table(table, client=client).to_yson_type()

    if get_backend_type(client=client) == "driver":
        raise YtError("Remote copy is not supported for native driver")

    # TODO(ignat): use base string in other places
    if isinstance(source_table, basestring):
        source_table = [source_table]

    destination_table = unlist(_prepare_destination_tables(destination_table, None, None,
                                                           client=client))

    # TODO(ignat): provide atomicity of attribute copying
    if copy_attributes:
        if len(source_table) != 1:
            raise YtError("Cannot copy attributes of multiple source tables")

        remote_proxy = get("//sys/clusters/{0}/proxy".format(cluster_name), client=client)
        current_proxy = get_config(client)["proxy"]["url"]
        current_token = get_config(client)["token"]

        get_config(client)["proxy"]["url"] = remote_proxy
        get_config(client)["token"] = get_value(remote_cluster_token, get_config(client)["token"])
        src_attributes = get(source_table[0] + "/@")

        get_config(client)["proxy"]["url"] = current_proxy
        get_config(client)["token"] = current_token
        attributes = src_attributes.get("user_attribute_keys", []) + \
                     ["compression_codec", "erasure_codec", "replication_factor"]
        for attribute in attributes:
            set_attribute(destination_table, attribute, src_attributes[attribute], client=client)

    spec = compose(
        lambda _: _configure_spec(_, client),
        lambda _: update({"network_name": network_name}, _) if network_name is not None else _,
        lambda _: update({"input_table_paths": map(get_input_name, source_table),
                          "output_table_path": destination_table.to_yson_type(),
                          "cluster_name": cluster_name},
                          _),
        lambda _: get_value(spec, {})
    )(spec)

    return _make_operation_request("remote_copy", spec, strategy, sync, client=client)
