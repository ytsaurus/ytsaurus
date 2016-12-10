from .driver import make_request
from .table_helpers import _prepare_format, _to_chunk_stream
from .common import set_option, bool_to_string
from .config import get_config
from .transaction_commands import _make_transactional_request
from .ypath import TablePath


def select_rows(query, timestamp=None, input_row_limit=None, output_row_limit=None, range_expansion_limit=None,
                fail_on_incomplete_result=None, verbose_logging=None, enable_code_cache=None, max_subqueries=None, workload_descriptor=None,
                format=None, raw=None, client=None):
    """Execute a SQL-like query on dynamic table.

    .. seealso:: `supported features <https://wiki.yandex-team.ru/yt/userdoc/queries>`_

    :param query: (string) for example \"<columns> [as <alias>], ... from \[<table>\] \
                  [where <predicate> [group by <columns> [as <alias>], ...]]\"
    :param timestamp: (int)
    :param format: (string or descendant of `Format`) output format
    :param raw: (bool) don't parse response to rows
    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]
    format = _prepare_format(format, raw, client)
    params = {
        "query": query,
        "output_format": format.to_yson_type()}
    set_option(params, "timestamp", timestamp)
    set_option(params, "input_row_limit", input_row_limit)
    set_option(params, "output_row_limit", output_row_limit)
    set_option(params, "range_expansion_limit", range_expansion_limit)
    set_option(params, "fail_on_incomplete_result", fail_on_incomplete_result, transform=bool_to_string)
    set_option(params, "verbose_logging", verbose_logging, transform=bool_to_string)
    set_option(params, "enable_code_cache", enable_code_cache, transform=bool_to_string)
    set_option(params, "max_subqueries", max_subqueries)
    set_option(params, "workload_descriptor", workload_descriptor)

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


def insert_rows(table, input_stream, update=None, aggregate=None, atomicity=None, durability=None,
                format=None, raw=None, client=None):
    """Insert rows from input_stream to dynamic table.

    :param table: (string or :py:class:`yt.wrapper.TablePath`) output table. Specify \
                `TablePath` attributes for append mode or something like this. Table can not exist.
    :param input_stream: python file-like object, string, list of strings, `StringIterIO`.
    :param format: (string or subclass of `Format`) format of input data, \
                    `yt.wrapper.config["tabular_data_format"]` by default.
    :param raw: (bool) if `raw` is specified stream with unparsed records (strings) \
                       in specified `format` is expected. Otherwise dicts or \
                       :class:`yt.wrapper.yamr_record.Record` are expected.

    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]

    table = TablePath(table, client=client)
    format = _prepare_format(format, raw, client)

    params = {}
    params["path"] = table
    params["input_format"] = format.to_yson_type()
    set_option(params, "update", update, transform=bool_to_string)
    set_option(params, "aggregate", aggregate, transform=bool_to_string)
    set_option(params, "atomicity", atomicity)
    set_option(params, "durability", durability)

    input_stream = _to_chunk_stream(input_stream, format, raw, split_rows=False, chunk_size=get_config(client)["write_retries"]["chunk_size"])

    _make_transactional_request(
        "insert_rows",
        params,
        data=input_stream,
        use_heavy_proxy=True,
        client=client)


def delete_rows(table, input_stream, atomicity=None, durability=None, format=None, raw=None, client=None):
    """Delete rows with keys from input_stream from dynamic table.

    :param table: (string or :py:class:`yt.wrapper.TablePath`) table to remove rows from.
    :param input_stream: python file-like object, string, list of strings, `StringIterIO`.
    :param format: (string or subclass of `Format`) format of input data, \
                    `yt.wrapper.config["tabular_data_format"]` by default.
    :param raw: (bool) if `raw` is specified stream with unparsed records (strings) \
                       in specified `format` is expected. Otherwise dicts or \
                       :class:`yt.wrapper.yamr_record.Record` are expected.

    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]

    table = TablePath(table, client=client)
    format = _prepare_format(format, raw, client)

    params = {}
    params["path"] = table
    params["input_format"] = format.to_yson_type()
    set_option(params, "atomicity", atomicity)
    set_option(params, "durability", durability)

    input_stream = _to_chunk_stream(input_stream, format, raw, split_rows=False, chunk_size=get_config(client)["write_retries"]["chunk_size"])

    _make_transactional_request(
        "delete_rows",
        params,
        data=input_stream,
        use_heavy_proxy=True,
        client=client)


def lookup_rows(table, input_stream, timestamp=None, column_names=None, keep_missing_rows=None,
                format=None, raw=None, client=None):
    """Lookup rows in dynamic table.

    .. seealso:: `supported features <https://wiki.yandex-team.ru/yt/userdoc/queries>`_

    :param format: (string or descendant of `Format`) output format
    :param raw: (bool) don't parse response to rows
    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]

    table = TablePath(table, client=client)
    format = _prepare_format(format, raw, client)

    params = {}
    params["path"] = table
    params["input_format"] = format.to_yson_type()
    params["output_format"] = format.to_yson_type()
    set_option(params, "timestamp", timestamp)
    set_option(params, "column_names", column_names)
    set_option(params, "keep_missing_rows", keep_missing_rows, transform=bool_to_string)

    input_stream = _to_chunk_stream(input_stream, format, raw, split_rows=False, chunk_size=get_config(client)["write_retries"]["chunk_size"])

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


def alter_table(path, schema=None, dynamic=None, client=None):
    """Sets schema of the dynamic table.

    :param table: string or `TablePath`
    :param schema: json-able object
    :param dynamic: (bool)
    """

    params = {"path": TablePath(path, client=client)}
    if schema is not None:
        params["schema"] = schema
    if dynamic is not None:
        params["dynamic"] = dynamic

    _make_transactional_request("alter_table", params, client=client)


def mount_table(path, first_tablet_index=None, last_tablet_index=None, cell_id=None,
                freeze=False, client=None):
    """Mount table.

    TODO
    """
    params = {"path": TablePath(path, client=client)}
    if first_tablet_index is not None:
        params["first_tablet_index"] = first_tablet_index
    if last_tablet_index is not None:
        params["last_tablet_index"] = last_tablet_index
    if cell_id is not None:
        params["cell_id"] = cell_id
    if freeze is not None:
        params["freeze"] = freeze

    make_request("mount_table", params, client=client)


def unmount_table(path, first_tablet_index=None, last_tablet_index=None, force=None, client=None):
    """Unmount table.

    TODO
    """
    params = {"path": TablePath(path, client=client)}
    if first_tablet_index is not None:
        params["first_tablet_index"] = first_tablet_index
    if last_tablet_index is not None:
        params["last_tablet_index"] = last_tablet_index
    if force is not None:
        params["force"] = bool_to_string(force)

    make_request("unmount_table", params, client=client)


def remount_table(path, first_tablet_index=None, last_tablet_index=None, client=None):
    """Remount table.

    TODO
    """
    params = {"path": path}
    if first_tablet_index is not None:
        params["first_tablet_index"] = first_tablet_index
    if last_tablet_index is not None:
        params["last_tablet_index"] = last_tablet_index

    make_request("remount_table", params, client=client)


def freeze_table(path, first_tablet_index=None, last_tablet_index=None, client=None):
    """Freeze table.

    TODO
    """
    params = {"path": TablePath(path, client=client)}
    if first_tablet_index is not None:
        params["first_tablet_index"] = first_tablet_index
    if last_tablet_index is not None:
        params["last_tablet_index"] = last_tablet_index

    make_request("freeze_table", params, client=client)


def unfreeze_table(path, first_tablet_index=None, last_tablet_index=None, client=None):
    """Unfreeze table.

    TODO
    """
    params = {"path": TablePath(path, client=client)}
    if first_tablet_index is not None:
        params["first_tablet_index"] = first_tablet_index
    if last_tablet_index is not None:
        params["last_tablet_index"] = last_tablet_index

    make_request("unfreeze_table", params, client=client)


def reshard_table(path, pivot_keys=None, tablet_count=None, first_tablet_index=None, last_tablet_index=None, client=None):
    """Change pivot keys separating tablets of a given table.

    TODO
    """
    params = {"path": TablePath(path, client=client)}

    set_option(params, "pivot_keys", pivot_keys)
    set_option(params, "tablet_count", tablet_count)
    set_option(params, "first_tablet_index", first_tablet_index)
    set_option(params, "last_tablet_index", last_tablet_index)

    make_request("reshard_table", params, client=client)
