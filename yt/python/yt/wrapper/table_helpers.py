from __future__ import print_function

from . import py_wrapper

from .batch_helpers import batch_apply, create_batch_client
from .common import (NullContext, update, get_value, chunk_iter_stream, require, get_disk_size,  # noqa
                     is_of_iterable_type, flatten, typing)  # noqa
from .config import get_config
from .errors import YtError
from .format import create_format, YsonFormat, YamrFormat, SkiffFormat, Format  # noqa
from .ypath import TablePath
from .cypress_commands import exists, get, get_attribute, remove
from .transaction_commands import abort_transaction
from .file_commands import upload_file_to_cache, is_executable, LocalFile
from .transaction import Transaction, null_transaction_id
from .skiff import convert_to_skiff_schema
from .stream import ItemStream
from .progress_bar import CustomTqdm

import yt.logger as logger
import yt.yson as yson

try:
    from yt.packages.six import text_type, binary_type, string_types
    from yt.packages.six.moves import map as imap, zip as izip
except ImportError:
    from six import text_type, binary_type, string_types
    from six.moves import map as imap, zip as izip

import os
import time
import types
from copy import deepcopy

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

import itertools

DEFAULT_EMPTY_TABLE = TablePath("//sys/empty_yamr_table", simplify=False)


def iter_by_chunks(iterable, count):
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, count - 1))


def _to_chunk_stream(stream, format, raw, split_rows, chunk_size, rows_chunk_size):
    # type: (str | bytes | "filelike" |  typing.Iterable, Format, bool, bool, int, int) -> ItemStream
    #
    # `raw`: read filelike or str|bytes input and produce:
    #   `split_rows` - parse raw input by Format and produce raw splitted by one recod (ignore `chunk_size`, `rows_chunk_size`)
    #     [b'<row1>', b'<row2>', b'<row3>'...]
    #   !`split_rows` -  hard split raw input by `chunk_size` (in bytes)
    #     [b'<row', b'1>,<', b'row2', b'>...]
    # !`raw`: read py objects and produce:
    #   `filelike` - error
    #   `split_rows` - serialize each row into Format (ignore `chunk_size`, `rows_chunk_size`)
    #     [b'<rec2>', b'<row2>', b'<row3>'...]
    #   !`split_rows` - group rows by `rows_chunk_size` (number, not bytes)
    #     [b'<rec2>,<row2>', b'<row3>,<row4', ...]
    if isinstance(stream, (text_type, binary_type)):
        if isinstance(stream, text_type):
            try:
                stream = stream.encode("ascii")
            except UnicodeDecodeError:
                raise YtError("Cannot split unicode string into chunks, consider encoding it to bytes first")
        stream = BytesIO(stream)

    is_iterable = is_of_iterable_type(stream)
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
    else:
        if is_filelike:
            raise YtError("Incorrect input type, it must be generator or list")
        if split_rows:
            # is_iterable
            stream = (format.dumps_row(row) for row in stream)
        else:
            stream = (format.dumps_rows(chunk) for chunk in iter_by_chunks(stream, rows_chunk_size))
    return ItemStream(stream)


def _prepare_command_format(format, raw, client):
    if format is None:
        format = get_config(client)["tabular_data_format"]
    if not raw and format is None:
        format = YsonFormat()
    if isinstance(format, string_types):
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

    existing_tables = []
    for table, exists_result in izip(tables, exists_results):
        if exists_result:
            existing_tables.append(table)

    type_results = batch_apply(
        lambda table, client: get(table + "/@type", client=client),
        existing_tables,
        client=client)

    tables_to_remove = []
    for table, table_type in izip(existing_tables, type_results):
        table = TablePath(table)
        if table_type == "table" and not table.append and table != DEFAULT_EMPTY_TABLE:
            if get_config(client)["yamr_mode"]["abort_transactions_with_remove"]:
                _remove_locks(table, client=client)
            tables_to_remove.append(table)

    batch_apply(remove, tables_to_remove, client=client)


class _MultipleFilesProgressBar(object):
    def __init__(self, total_size, file_count, enable):
        self.total_size = total_size
        self.file_count = file_count
        self.enable = enable
        self._tqdm = None
        self._current_file_index = 0
        self._current_filename = None

    def __enter__(self):
        bar_format = "{l_bar}{bar}| {n_fmt}/{total_fmt}"
        if self.enable is None:
            disable = None
        else:
            disable = not self.enable
        self._tqdm = CustomTqdm(disable=disable, bar_format=bar_format, total=self.total_size)
        self._tqdm.set_description("Starting upload")
        return self

    def __exit__(self, type, value, traceback):
        self._tqdm.close()
        self._tqdm = None

    def _set_status(self, status):
        if self._tqdm is None:
            return
        self._tqdm.set_description("({}/{}) [{}] {}".format(self._current_file_index, self.file_count, status.upper(),
                                                            os.path.basename(self._current_filename)))

    def monitor(self, filename):
        if isinstance(filename, LocalFile):
            filename = filename.path
        self._current_file_index += 1
        self._current_filename = filename
        return _UploadProgressMonitor(self)


class _UploadProgressMonitor(object):
    def __init__(self, bar):
        self._bar = bar

    def start(self):
        self._bar._set_status("upload")

    def update(self, size):
        self._bar._tqdm.update(size)

    def finish(self, status="ok"):
        self._bar._set_status(status)


class FileManager(object):
    def __init__(self, client):
        self.client = client
        self.disk_size = 0
        self.local_size = 0
        self.files = []
        self.uploaded_files = []

    def add_files(self, files):
        for file in flatten(files):
            if isinstance(file, (text_type, binary_type, LocalFile)):
                file_params = {"filename": file}
            else:
                file_params = deepcopy(file)
            filename = file_params["filename"]
            local_file = LocalFile(filename)
            self.local_size += get_disk_size(local_file.path, round=False)
            self.disk_size += get_disk_size(local_file.path)
            self.files.append(file_params)

    def upload_files(self):
        if not self.files:
            return []

        enable_progress_bar = get_config(self.client)["write_progress_bar"]["enable"]
        if enable_progress_bar is False:
            bar = NullContext()
        else:
            bar = _MultipleFilesProgressBar(self.local_size, len(self.files), enable_progress_bar)

        file_paths = []
        with bar:
            with Transaction(transaction_id=null_transaction_id,
                             attributes={"title": "Python wrapper: upload operation files"},
                             client=self.client):
                for file_params in self.files:
                    filename = file_params.pop("filename")
                    local_file = LocalFile(filename)
                    if enable_progress_bar is False:
                        upload_monitor = None
                    else:
                        upload_monitor = bar.monitor(filename)
                    path = upload_file_to_cache(filename=local_file.path,
                                                progress_monitor=upload_monitor,
                                                client=self.client,
                                                **file_params)
                    file_paths.append(yson.to_yson_type(path, attributes=update(
                        {
                            "executable": is_executable(local_file.path, client=self.client),
                        },
                        local_file.attributes
                    )))
                    self.uploaded_files.append(path)
        return file_paths


def _is_python_function(binary):
    return isinstance(binary, types.FunctionType) or hasattr(binary, "__call__")


def _prepare_format(format, default_format=None):
    if format is None:
        return default_format
    if isinstance(format, string_types):
        return create_format(format)
    return format


def _prepare_format_from_binary(format, binary, format_type):
    format_from_binary = getattr(binary, "attributes", {}).get(format_type, None)
    if format is None:
        return format_from_binary
    if format_from_binary is None:
        return format
    raise YtError("'{}' specified both implicitly and as function attribute".format(format_type))


def _get_skiff_schema_from_tables(tables, client):
    def _get_schema(table):
        if table is None:
            return None
        try:
            schema = get(table + "/@schema", client=client)
            rename_columns = table.attributes.get("rename_columns")
            if rename_columns is not None:
                for column in schema:
                    if column["name"] in rename_columns:
                        column["name"] = rename_columns[column["name"]]
            return schema
        except YtError as err:
            if err.is_resolve_error():
                return None
            raise

    schemas = []
    for table in tables:
        schema = _get_schema(table)
        if schema is None:
            return None
        schemas.append(schema)
    return list(imap(convert_to_skiff_schema, schemas))


def _prepare_default_format(binary, format_type, tables, client):
    is_python_function = _is_python_function(binary)
    if is_python_function and getattr(binary, "attributes", {}).get("with_skiff_schemas", False):
        skiff_schema = _get_skiff_schema_from_tables(tables, client)
        if skiff_schema is not None:
            return SkiffFormat(skiff_schema)
    format = _prepare_format(get_config(client)["tabular_data_format"])
    if format is None and is_python_function:
        return YsonFormat()
    if format is None:
        raise YtError("You should specify " + format_type)
    return format


def _prepare_operation_formats(format, input_format, output_format, binary, input_tables, output_tables, client):
    format = _prepare_format(format)
    input_format = _prepare_format(input_format, format)
    output_format = _prepare_format(output_format, format)

    input_format = _prepare_format_from_binary(input_format, binary, "input_format")
    output_format = _prepare_format_from_binary(output_format, binary, "output_format")

    if input_format is None:
        input_format = _prepare_default_format(binary, "input_format", input_tables, client)
    if output_format is None:
        output_format = _prepare_default_format(binary, "output_format", output_tables, client)

    return input_format, output_format


def _prepare_python_command(binary, file_manager, tempfiles_manager, params, local_mode, client=None):
    start_time = time.time()
    if isinstance(params.input_format, YamrFormat) and params.group_by is not None and set(params.group_by) != {"key"}:
        raise YtError("Yamr format does not support reduce by %r", params.group_by)
    result = py_wrapper.wrap(
        function=binary,
        file_manager=file_manager,
        tempfiles_manager=tempfiles_manager,
        local_mode=local_mode,
        params=params,
        client=client)

    logger.debug("Collecting python modules and uploading to cypress takes %.2lf seconds", time.time() - start_time)

    return result


def _prepare_destination_tables(tables, create_on_cluster=False, client=None):
    from .table_commands import _create_table, _merge_with_create_table_default_attributes
    if tables is None:
        if get_config(client)["yamr_mode"]["throw_on_missing_destination"]:
            raise YtError("Destination tables are missing")
        return []
    tables = list(imap(lambda name: TablePath(name, client=client), flatten(tables)))
    if create_on_cluster:
        def make_table_path(path, client):
            table = TablePath(path)
            default_attributes = _merge_with_create_table_default_attributes(attributes=None, client=client)

            # NB(coteeq): Default attributes have lower priority here,
            #             because controller overrides them when operation ends.
            table._path_object.attributes = update(default_attributes, table._path_object.attributes)
            table._path_object.attributes["create"] = True

            return table

        return [
            make_table_path(table, client=client)
            for table in tables
        ]

    batch_client = create_batch_client(raise_errors=True, client=client)
    for table in tables:
        _create_table(table, ignore_existing=True, client=batch_client)
    batch_client.commit_batch()
    return tables


def _prepare_job_io(job_io=None, table_writer=None):
    if job_io is None:
        job_io = {}
    if table_writer is not None:
        job_io.setdefault("table_writer", table_writer)
    return job_io


def _prepare_operation_files(local_files=None, yt_files=None):
    result = []

    if yt_files is not None:
        result += flatten(yt_files)

    local_files = flatten(get_value(local_files, []))
    result += map(LocalFile, local_files)
    return result


def _prepare_stderr_table(name, client=None):
    from .table_commands import _create_table
    if name is None:
        return None

    table = TablePath(name, client=client)
    with Transaction(transaction_id=null_transaction_id, client=client):
        _create_table(table, ignore_existing=True, client=client)
    return table
