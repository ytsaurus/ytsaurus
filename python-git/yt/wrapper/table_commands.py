from .common import flatten, require, update, parse_bool, get_value, set_param, \
                    MB, EMPTY_GENERATOR
from .config import get_config
from .cypress_commands import exists, remove, get_attribute, copy, \
                              move, mkdir, find_free_subpath, create, get, has_attribute
from .driver import make_request
from .errors import YtIncorrectResponse, YtError
from .format import create_format, YsonFormat
from .heavy_commands import make_write_request, make_read_request
from .table_helpers import _prepare_source_tables, _are_default_empty_table, _prepare_table_writer, \
                           _remove_tables, DEFAULT_EMPTY_TABLE, _to_chunk_stream, _prepare_format
from .ypath import TablePath, ypath_join

import yt.json as json
import yt.yson as yson
import yt.logger as logger
from yt.packages.six import PY3
from yt.packages.six.moves import map as imap, filter as ifilter

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

# Auxiliary methods

def _get_format_from_tables(tables, ignore_unexisting_tables):
    """Try to get format from tables, raise YtError if tables have different _format attribute"""
    not_none_tables = list(ifilter(None, flatten(tables)))

    if ignore_unexisting_tables:
        tables_to_extract = list(ifilter(lambda x: exists(TablePath(x)), not_none_tables))
    else:
        tables_to_extract = not_none_tables

    if not tables_to_extract:
        return None

    def extract_format(table):
        table = TablePath(table)

        if not exists(table):
            return None

        if has_attribute(table, "_format"):
            format_name = get(table + "/@_format", format=YsonFormat())
            return create_format(format_name)
        return None

    formats = list(imap(extract_format, tables_to_extract))

    def format_repr(format):
        if format is not None:
            return yson.dumps(format._name, boolean_as_string=True)
        return repr(None)

    require(len(set(format_repr(format) for format in formats)) == 1,
            lambda: YtError("Tables have different attribute _format: " + repr(formats)))

    return formats[0]

""" Common table methods """

def create_table(path, recursive=None, ignore_existing=False,
                 attributes=None, client=None):
    """Create empty table.

    Shortcut for `create("table", ...)`.
    :param path: (string or :py:class:`yt.wrapper.TablePath`) path to table
    :param recursive: (bool) create the path automatically, `config["yamr_mode"]["create_recursive"]` by default
    :param ignore_existing: (bool) if it sets to `False` and table exists, \
                            Python Wrapper raises `YtResponseError`.
    :param attributes: (dict)
    """
    table = TablePath(path, client=client)
    attributes = get_value(attributes, {})
    if get_config(client)["create_table_attributes"] is not None:
        attributes = update(get_config(client)["create_table_attributes"], attributes)
    if get_config(client)["yamr_mode"]["use_yamr_defaults"]:
        attributes = update({"compression_codec": "zlib_6"}, attributes)
    create("table", table, recursive=recursive, ignore_existing=ignore_existing,
           attributes=attributes, client=client)

def create_temp_table(path=None, prefix=None, attributes=None, client=None):
    """Create temporary table by given path with given prefix and return name.

    :param path: (string or :py:class:`yt.wrapper.TablePath`) existing path, \
                 by default `config["remote_temp_tables_directory"]`
    :param prefix: (string) prefix of table name
    :return: (string) name of result table
    """
    if path is None:
        path = get_config(client)["remote_temp_tables_directory"]
        mkdir(path, recursive=True, client=client)
    else:
        path = str(TablePath(path, client=client))
    require(exists(path, client=client), lambda: YtError("You cannot create table in unexisting path"))
    if prefix is not None:
        path = ypath_join(path, prefix)
    else:
        if not path.endswith("/"):
            path = path + "/"
    name = find_free_subpath(path, client=client)
    create_table(name, attributes=attributes, client=client)
    return name

def write_table(table, input_stream, format=None, table_writer=None,
                is_stream_compressed=False, force_create=None, raw=None,
                client=None):
    """Write rows from input_stream to table.

    :param table: (string or :py:class:`yt.wrapper.TablePath`) output table. Specify \
                `TablePath` attributes for append mode or something like this. Table can not exist.
    :param input_stream: python file-like object, string, list of strings, `StringIterIO`.
    :param format: (string or subclass of `Format`) format of input data, \
                    `yt.wrapper.config["tabular_data_format"]` by default.
    :param table_writer: (dict) spec of "write" operation
    :param is_stream_compressed: (bool) expect stream to contain compressed table data. \
    This data can be passed directly to proxy without recompression. Be careful! this option \
    disables write retries.
    :param force_create: (bool) unconditionally creates table and ignores existing table.

    Python Wrapper try to split input stream to portions of fixed size and write its with retries.
    If splitting fails, stream is written as is through HTTP.
    Set `yt.wrapper.config["write_retries"]["enable"]` to ``False`` for writing \
    without splitting and retries.

    Writing is executed under self-pinged transaction.
    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]

    if force_create is None:
        force_create = True

    if is_stream_compressed and not raw:
        raise YtError("Compressed stream is only supported for raw tabular data")

    table = TablePath(table, client=client)
    format = _prepare_format(format, raw, client)
    table_writer = _prepare_table_writer(table_writer, client)

    params = {}
    params["input_format"] = format.to_yson_type()
    set_param(params, "table_writer", table_writer)

    def prepare_table(path):
        if not force_create:
            return
        create_table(path, ignore_existing=True, client=client)

    can_split_input = isinstance(input_stream, list) or format.is_raw_load_supported()
    enable_retries = get_config(client)["write_retries"]["enable"] and \
            can_split_input and \
            not is_stream_compressed
    if get_config(client)["write_retries"]["enable"] and not can_split_input:
        logger.warning("Cannot split input into rows. Write is processing by one request.")

    input_stream = _to_chunk_stream(
        input_stream,
        format,
        raw,
        split_rows=enable_retries,
        chunk_size=get_config(client)["write_retries"]["chunk_size"])

    make_write_request(
        "write_table",
        input_stream,
        table,
        params,
        prepare_table,
        use_retries=enable_retries,
        is_stream_compressed=is_stream_compressed,
        client=client)

    if get_config(client)["yamr_mode"]["delete_empty_tables"] and is_empty(table, client=client):
        _remove_tables([table], client=client)

def read_table(table, format=None, table_reader=None, control_attributes=None, unordered=None,
               raw=None, response_parameters=None, read_transaction=None, client=None):
    """Read rows from table and parse (optionally).

    :param table: string or :py:class:`yt.wrapper.TablePath`
    :param table_reader: (dict) spec of "read" operation
    :param raw: (bool) don't parse response to rows
    :return: if `raw` is specified -- string or :class:`yt.wrapper.driver.ResponseStream`,\
             else -- rows generator (python dict or :class:`yt.wrapper.yamr_record.Record`)

    If :py:data:`yt.wrapper.config["read_retries"]["enable"]` is specified,
    command is executed under self-pinged transaction with retries and snapshot lock on the table.
    This transaction is alive until your finish reading your table, or call `close` method of ResponseStream.
    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]

    table = TablePath(table, client=client)
    format = _prepare_format(format, raw, client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not exists(table, client=client):
        return BytesIO() if raw else EMPTY_GENERATOR
    attributes = get(table + "/@", client=client)
    if attributes.get("type") != "table":
        raise YtError("Command read is supported only for tables")
    if  attributes["chunk_count"] > 100 and attributes["compressed_data_size"] // attributes["chunk_count"] < MB:
        logger.info("Table chunks are too small; consider running the following command to improve read performance: "
                    "yt merge --proxy {1} --src {0} --dst {0} "
                    "--spec '{{"
                       "combine_chunks=true;"
                    "}}'".format(table, get_config(client)["proxy"]["url"]))

    params = {
        "path": table,
        "output_format": format.to_yson_type()
    }
    set_param(params, "table_reader", table_reader)
    set_param(params, "control_attributes", control_attributes)
    set_param(params, "unordered", unordered)

    def set_response_parameters(parameters):
        if response_parameters is not None:
            for key in parameters:
                response_parameters[key] = parameters[key]

    def process_response(response):
        if response.response_parameters is None:
            raise YtIncorrectResponse("X-YT-Response-Parameters missing (bug in proxy)", response._get_response())
        set_response_parameters(response.response_parameters)

    class RetriableState(object):
        def __init__(self):
            # Whether reading started, it is used only for reading without ranges in <= 0.17.3 versions.
            self.started = False

            # Row and range indices.
            self.next_row_index = None
            self.current_range_index = 0

            # It is true if and only if we read control attributes of the current range and the next range isn't started yet.
            self.range_started = None

            # We should know whether row with range/row index printed.
            self.range_index_row_yielded = None
            self.row_index_row_yielded = None

            self.is_row_index_initially_enabled = False
            if control_attributes and control_attributes.get("enable_row_index"):
                self.is_row_index_initially_enabled = True

            self.is_range_index_initially_enabled = False
            if control_attributes and control_attributes.get("enable_range_index"):
                self.is_range_index_initially_enabled = True

            if unordered:
                raise YtError("Unordered read cannot be performed with retries, try ordered read or disable retries")

        def prepare_params_for_retry(self):
            if "ranges" not in table.attributes:
                if self.started:
                    table.attributes["lower_limit"] = {"row_index": self.next_row_index}
            else:
                if len(table.attributes["ranges"]) > 1:
                    if get_config(client)["read_retries"]["allow_multiple_ranges"]:
                        if "control_attributes" not in params:
                            params["control_attributes"] = {}
                        params["control_attributes"]["enable_row_index"] = True
                        params["control_attributes"]["enable_range_index"] = True
                    else:
                        raise YtError("Read table with multiple ranges using retries is disabled, turn on read_retries/allow_multiple_ranges")

                    if format.name() not in ["json", "yson"]:
                        raise YtError("Read table with multiple ranges using retries is supported only in YSON and JSON formats")
                    if format.name() == "json" and format.attributes.get("format") == "pretty":
                        raise YtError("Read table with multiple ranges using retries is not supported for pretty JSON format")

                if self.range_started and table.attributes["ranges"]:
                    table.attributes["ranges"][0]["lower_limit"] = {"row_index": self.next_row_index}
                self.range_started = False

            params["path"] = table

            return params


        def iterate(self, response):
            format_name = format.name()

            def is_control_row(row):
                if format_name == "yson":
                    return row.endswith(b"#;")
                elif format_name == "json":
                    if b"$value" not in row:
                        return False
                    loaded_row = json.loads(row)
                    return "$value" in loaded_row and loaded_row["$value"] is None
                else:
                    return False

            def load_control_row(row):
                if format_name == "yson":
                    return next(yson.loads(row, yson_type="list_fragment"))
                elif format_name == "json":
                    return yson.json_to_yson(json.loads(row))
                else:
                    assert False, "Incorrect format"

            def dump_control_row(row):
                if format_name == "yson":
                    return yson.dumps([row], yson_type="list_fragment")
                elif format_name == "json":
                    row = json.dumps(yson.yson_to_json(row))
                    if PY3:
                        row = row.encode("utf-8")
                    return row + b"\n"
                else:
                    assert False, "Incorrect format"

            range_index = 0

            if not self.started:
                process_response(response)
                self.next_row_index = response.response_parameters.get("start_row_index", None)
                self.started = True

            for row in format.load_rows(response, raw=True):
                # NB: Low level check for optimization purposes. Only YSON and JSON format supported!
                if is_control_row(row):
                    row = load_control_row(row)

                    # NB: row with range index must go before row with row index.
                    if hasattr(row, "attributes") and "range_index" in row.attributes:
                        self.range_started = False
                        ranges_to_skip = row.attributes["range_index"] - range_index
                        table.attributes["ranges"] = table.attributes["ranges"][ranges_to_skip:]
                        self.current_range_index += ranges_to_skip
                        range_index = row.attributes["range_index"]
                        if not self.is_range_index_initially_enabled:
                            del row.attributes["range_index"]
                            assert not row.attributes
                            continue
                        else:
                            if self.range_index_row_yielded:
                                continue
                            row.attributes["range_index"] = self.current_range_index
                            self.range_index_row_yielded = True

                    if hasattr(row, "attributes") and "row_index" in row.attributes:
                        self.next_row_index = row.attributes["row_index"]
                        if not self.is_row_index_initially_enabled:
                            del row.attributes["row_index"]
                            assert not row.attributes
                            continue
                        else:
                            if self.row_index_row_yielded:
                                continue
                            self.row_index_row_yielded = True

                    row = dump_control_row(row)
                else:
                    if not self.range_started:
                        self.range_started = True
                        self.range_index_row_yielded = False
                        self.row_index_row_yielded = False
                    self.next_row_index += 1

                yield row

    # For read commands response is actually ResponseStream
    response = make_read_request(
        "read_table",
        table,
        params,
        process_response_action=process_response,
        retriable_state_class=RetriableState,
        client=client)

    if raw:
        return response
    else:
        return format.load_rows_with_finalization(response, on_close=lambda: response.close())

def _are_valid_nodes(source_tables, destination_table):
    return len(source_tables) == 1 and \
           not source_tables[0].has_delimiters() and \
           not destination_table.append and \
           destination_table != source_tables[0]

def copy_table(source_table, destination_table, replace=True, client=None):
    """Copy table(s).

    :param source_table: string, `TablePath` or list of them
    :param destination_table: string or `TablePath`
    :param replace: (bool) override `destination_table`

    .. note:: param `replace` is overridden by set \
              `yt.wrapper.config["yamr_mode"]["replace_tables_on_copy_and_move"]`
    If `source_table` is a list of tables, tables would be merged.
    """
    from .run_operation_commands import run_merge
    if get_config(client)["yamr_mode"]["replace_tables_on_copy_and_move"]:
        replace = True
    source_tables = _prepare_source_tables(source_table, client=client)
    destination_table = TablePath(destination_table, client=client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and \
            _are_default_empty_table(source_tables) and \
            not destination_table.append:
        remove(destination_table, client=client, force=True)
        return
    if _are_valid_nodes(source_tables, destination_table):
        if replace and \
                exists(destination_table, client=client) and \
                source_tables[0] != destination_table:
            # in copy destination should be missing
            remove(destination_table, client=client)
        copy(source_tables[0], destination_table, recursive=True, client=client)
    else:
        mode = "sorted" if (all(imap(lambda t: is_sorted(t, client=client), source_tables)) and not destination_table.append) \
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
    destination_table = TablePath(destination_table, client=client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and \
            _are_default_empty_table(source_tables) and \
            not destination_table.append:
        remove(destination_table, client=client, force=True)
        return
    if _are_valid_nodes(source_tables, destination_table):
        if source_tables[0] == destination_table:
            return
        if replace and exists(destination_table, client=client):
            remove(destination_table, client=client)
        move(source_tables[0], destination_table, recursive=True, client=client)
    else:
        copy_table(source_table, destination_table, client=client)
        for table in source_tables:
            if table == destination_table:
                continue
            if table == DEFAULT_EMPTY_TABLE:
                continue
            remove(table, client=client, force=True)


def records_count(table, client=None):
    """ Deprecated!

    Return number of records in the table.

    :param table: string or `TablePath`
    :return: integer
    """
    return row_count(table, client)

def row_count(table, client=None):
    """Return number of rows in the table.

    :param table: string or `TablePath`
    :return: integer
    """
    table = TablePath(table, client=client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not exists(table, client=client):
        return 0
    return get_attribute(table, "row_count", client=client)

def is_empty(table, client=None):
    """Is table empty?

    :param table: (string or `TablePath`)
    :return: (bool)
    """
    return row_count(TablePath(table, client=client), client=client) == 0

def get_sorted_by(table, default=None, client=None):
    """Get 'sorted_by' table attribute or `default` if attribute doesn't exist.

    :param table: string or `TablePath`
    :param default: whatever
    :return: string or list of string
    """
    if default is None:
        default = [] if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] else None
    return get_attribute(TablePath(table, client=client), "sorted_by", default=default, client=client)

def is_sorted(table, client=None):
    """Is table sorted?

    :param table: string or `TablePath`
    :return: bool
    """
    if get_config(client)["yamr_mode"]["use_yamr_sort_reduce_columns"]:
        return get_sorted_by(table, [], client=client) == ["key", "subkey"]
    else:
        return parse_bool(
            get_attribute(TablePath(table, client=client),
                          "sorted",
                          default="false",
                          client=client))

def enable_table_replica(replica_id, client=None):
    """ TODO """
    return make_request("enable_table_replica", params={"replica_id": replica_id}, client=client)

def disable_table_replica(replica_id, client=None):
    """ TODO """
    return make_request("disable_table_replica", params={"replica_id": replica_id}, client=client)
