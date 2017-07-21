from . import py_wrapper

from .batch_helpers import batch_apply, create_batch_client
from .common import flatten, update, get_value, chunk_iter_stream, require, get_disk_size
from .config import get_config
from .errors import YtError
from .format import create_format, YsonFormat, YamrFormat
from .ypath import TablePath
from .cypress_commands import exists, get_attribute, get_type, remove
from .transaction_commands import abort_transaction
from .file_commands import upload_file_to_cache, is_executable
from .transaction import Transaction, null_transaction_id

from yt.common import to_native_str

import yt.logger as logger
import yt.yson as yson
from yt.yson.parser import YsonParser

from yt.packages.six import text_type, binary_type, PY3
from yt.packages.six.moves import map as imap, zip as izip

import os
import time
import types
from copy import deepcopy

try:
    # It is used to checks iterable type.
    # Since isinstance(StringIO, collections.Iterator) raises an error in python 2.6.
    from StringIO import StringIO as pythonStringIO
except ImportError:  # Python 3
    pythonStringIO = None

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

import collections

DEFAULT_EMPTY_TABLE = TablePath("//sys/empty_yamr_table", simplify=False)

def _to_chunk_stream(stream, format, raw, split_rows, chunk_size):
    if isinstance(stream, (text_type, binary_type)):
        if isinstance(stream, text_type):
            if not PY3:
                stream = stream.encode("utf-8")
            else:
                raise YtError("Cannot split unicode string into chunks, consider encoding it first")
        stream = BytesIO(stream)

    iterable_types = [list, types.GeneratorType, collections.Iterator, collections.Iterable]
    if pythonStringIO is not None:
        iterable_types.insert(0, pythonStringIO)

    is_iterable = isinstance(stream, tuple(iterable_types))
    is_filelike = hasattr(stream, "read")

    if not is_iterable and not is_filelike:
        raise YtError("Cannot split stream into chunks. "
                      "Expected iterable or file-like object, got {0}".format(repr(stream)))

    if raw:
        if is_filelike:
            if split_rows:
                stream = format.load_rows(stream, raw=True)
            else:
                stream = chunk_iter_stream(stream, chunk_size)
        for chunk in stream:
            yield chunk
    else:
        if is_filelike:
            raise YtError("Incorrect input type, it must be generator or list")
        # is_iterable
        for row in stream:
            yield format.dumps_row(row)


def _prepare_format(format, raw, client):
    if format is None:
        format = get_config(client)["tabular_data_format"]
    if not raw and format is None:
        format = YsonFormat(process_table_index=False, boolean_as_string=False)
    if isinstance(format, str):
        format = create_format(format)

    require(format is not None,
            lambda: YtError("You should specify format"))
    return format

def _prepare_source_tables(tables, replace_unexisting_by_empty=True, client=None):
    result = [TablePath(table, client=client) for table in flatten(tables)]
    if not result:
        raise YtError("You must specify non-empty list of source tables")
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"]:
        filtered_result = []
        exists_results = batch_apply(exists, result, client=client)

        for table, exists_result in izip(result, exists_results):
            if exists_result:
                filtered_result.append(table)
            else:
                logger.warning("Warning: input table '%s' does not exist", table)
                if replace_unexisting_by_empty:
                    filtered_result.append(DEFAULT_EMPTY_TABLE)
        result = filtered_result
    return result


def _are_default_empty_table(tables):
    return all(table == DEFAULT_EMPTY_TABLE for table in tables)


def _prepare_table_writer(table_writer, client):
    table_writer_from_config = deepcopy(get_config(client)["table_writer"])
    if table_writer is None and not table_writer_from_config:
        return None
    return update(table_writer_from_config, get_value(table_writer, {}))


def _remove_locks(table, client=None):
    for lock_obj in get_attribute(table, "locks", [], client=client):
        if lock_obj["mode"] != "snapshot":
            if exists("//sys/transactions/" + lock_obj["transaction_id"], client=client):
                abort_transaction(lock_obj["transaction_id"], client=client)


def _remove_tables(tables, client=None):
    exists_results = batch_apply(exists, tables, client=client)

    exists_tables = []
    for table, exists_result in izip(tables, exists_results):
        if exists_result:
            exists_tables.append(table)

    type_results = batch_apply(get_type, tables, client=client)

    tables_to_remove = []
    for table, table_type in izip(exists_tables, type_results):
        table = TablePath(table)
        if table_type == "table" and not table.append and table != DEFAULT_EMPTY_TABLE:
            if get_config(client)["yamr_mode"]["abort_transactions_with_remove"]:
                _remove_locks(table, client=client)
            tables_to_remove.append(table)

    batch_apply(remove, tables_to_remove, client=client)

class FileUploader(object):
    def __init__(self, client):
        self.client = client
        self.disk_size = 0

    def __call__(self, files):
        if files is None:
            return []

        file_paths = []
        with Transaction(transaction_id=null_transaction_id, attributes={"title": "Python wrapper: upload operation files"}, client=self.client):
            for file in flatten(files):
                if isinstance(file, (text_type, binary_type)):
                    file_params = {"filename": file}
                else:
                    file_params = deepcopy(file)

                # Hacky way to split string into file path and file path attributes.
                filename = file_params.pop("filename")
                if PY3:
                    filename_bytes = filename.encode("utf-8")
                else:
                    filename_bytes = filename

                stream = BytesIO(filename_bytes)
                parser = YsonParser(
                    stream,
                    encoding="utf-8" if PY3 else None,
                    always_create_attributes=True)

                attributes = {}
                if parser._has_attributes():
                    attributes = parser._parse_attributes()
                    filename = to_native_str(stream.read())

                self.disk_size += get_disk_size(filename)

                path = upload_file_to_cache(filename=filename, client=self.client, **file_params)
                file_paths.append(yson.to_yson_type(path, attributes={
                    "executable": is_executable(filename, client=self.client),
                    "file_name": attributes.get("file_name", os.path.basename(filename)),
                }))
        return file_paths

def _is_python_function(binary):
    return isinstance(binary, types.FunctionType) or hasattr(binary, "__call__")

def _prepare_formats(format, input_format, output_format, binary, client):
    if format is None:
        format = get_config(client)["tabular_data_format"]
    if format is None and _is_python_function(binary):
        format = YsonFormat(boolean_as_string=False)
    if isinstance(format, str):
        format = create_format(format)
    if isinstance(input_format, str):
        input_format = create_format(input_format)
    if isinstance(output_format, str):
        output_format = create_format(output_format)

    if input_format is None:
        input_format = format
    require(input_format is not None,
            lambda: YtError("You should specify input format"))

    if output_format is None:
        output_format = format
    require(output_format is not None,
            lambda: YtError("You should specify output format"))

    return input_format, output_format

def _prepare_binary(binary, operation_type, input_format, output_format,
                    group_by, file_uploader, client=None):
    if _is_python_function(binary):
        start_time = time.time()
        if isinstance(input_format, YamrFormat) and group_by is not None and set(group_by) != set(["key"]):
            raise YtError("Yamr format does not support reduce by %r", group_by)
        wrap_result = \
            py_wrapper.wrap(function=binary,
                            operation_type=operation_type,
                            input_format=input_format,
                            output_format=output_format,
                            group_by=group_by,
                            uploader=file_uploader,
                            client=client)

        logger.debug("Collecting python modules and uploading to cypress takes %.2lf seconds", time.time() - start_time)
        return wrap_result
    else:
        return py_wrapper.WrapResult(cmd=binary, files=[], tmpfs_size=0, environment={}, local_files_to_remove=[], title=None)

def _prepare_destination_tables(tables, client=None):
    if tables is None:
        if get_config(client)["yamr_mode"]["throw_on_missing_destination"]:
            raise YtError("Destination tables are missing")
        return []
    tables = list(imap(lambda name: TablePath(name, client=client), flatten(tables)))
    batch_client = create_batch_client(raise_errors=True, client=client)
    for table in tables:
        batch_client.create_table(table, ignore_existing=True)
    batch_client.commit_batch()
    return tables

def _prepare_job_io(job_io=None, table_writer=None):
    if job_io is None:
        job_io = {}
    if table_writer is not None:
        job_io.setdefault("table_writer", table_writer)
    return job_io

def _prepare_local_files(local_files=None, files=None):
    if files is not None:
        require(local_files is None, lambda: YtError("You cannot specify files and local_files simultaneously"))
        local_files = files
    return local_files

def _prepare_stderr_table(name, client=None):
    from .table_commands import create_table
    if name is None:
        return None

    table = TablePath(name, client=client)
    with Transaction(transaction_id=null_transaction_id, client=client):
        create_table(table, ignore_existing=True, client=client)
    return table
