from .batch_helpers import create_batch_client, batch_apply
from .common import flatten, update, get_value, chunk_iter_stream, require
from .config import get_config
from .errors import YtError
from .format import create_format, YsonFormat
from .ypath import TablePath
from .cypress_commands import exists, get_attribute, get_type
from .transaction_commands import abort_transaction

import yt.logger as logger
from yt.packages.six.moves import zip as izip
from yt.packages.six import text_type, binary_type, PY3

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
            if exists_result.get_result():
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
    for table, table_type in izip(exists_tables, type_results):
        if table_type == "table" and not table.append and table != DEFAULT_EMPTY_TABLE:
            if get_config(client)["yamr_mode"]["abort_transactions_with_remove"]:
                _remove_locks(table, client=client)
            batch_client.remove(table)
    batch_client.commit_batch()
