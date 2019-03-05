from .common import (flatten, require, update, get_value, set_param, datetime_to_string,
                     MB, chunk_iter_stream, deprecated)
from .config import get_config, get_option
from .cypress_commands import (exists, remove, get_attribute, copy,
                               move, mkdir, find_free_subpath, create, get, has_attribute)
from .driver import make_request, _create_http_client_from_rpc
from .retries import default_chaos_monkey, run_chaos_monkey
from .errors import YtIncorrectResponse, YtError, YtResponseError
from .format import create_format, YsonFormat
from .batch_response import apply_function_to_result
from .heavy_commands import make_write_request, make_read_request
from .parallel_writer import make_parallel_write_request
from .response_stream import EmptyResponseStream, ResponseStreamWithReadRow
from .table_helpers import (_prepare_source_tables, _are_default_empty_table, _prepare_table_writer,
                            _remove_tables, DEFAULT_EMPTY_TABLE, _to_chunk_stream, _prepare_command_format)
from .file_commands import _get_remote_temp_files_directory
from .parallel_reader import make_read_parallel_request
from .ypath import TablePath, ypath_join

import yt.json_wrapper as json
import yt.yson as yson
import yt.logger as logger
from yt.packages.six import PY3
from yt.packages.six.moves import map as imap, filter as ifilter, xrange

from copy import deepcopy
from datetime import datetime, timedelta

# Auxiliary methods

def _get_format_from_tables(tables, ignore_unexisting_tables):
    """Tries to get format from tables, raises :class:`YtError <yt.common.YtError>` if tables \
       have different _format attribute."""
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
            format_name = get(table + "/@_format", format=YsonFormat(require_yson_bindings=False))
            return create_format(format_name)
        return None

    formats = list(imap(extract_format, tables_to_extract))

    def format_repr(format):
        if format is not None:
            return yson.dumps(format._name)
        return repr(None)

    require(len(set(format_repr(format) for format in formats)) == 1,
            lambda: YtError("Tables have different attribute _format: " + repr(formats)))

    return formats[0]

def _create_table(path, recursive=None, ignore_existing=False, attributes=None, client=None):
    table = TablePath(path, client=client)
    attributes = get_value(attributes, {})
    if get_config(client)["create_table_attributes"] is not None:
        attributes = update(get_config(client)["create_table_attributes"], attributes)
    if get_config(client)["yamr_mode"]["use_yamr_defaults"]:
        attributes = update({"compression_codec": "zlib_6"}, attributes)
    return create("table", table, recursive=recursive, ignore_existing=ignore_existing,
                  attributes=attributes, client=client)

def _with_schema_if_exists(table_path):
    """Fetches schema of a given table and, if table exists and has schema, applies it
       to `table_path`."""
    try:
        schema = get(table_path + "/@schema")
    except YtResponseError as err:
        if err.is_resolve_error():
            return table_path
        else:
            raise
    else:
        return TablePath(table_path, schema=schema)

@deprecated(alternative='"create" with "table" type')
def create_table(path, recursive=None, ignore_existing=False,
                 attributes=None, client=None):
    """Creates empty table. Shortcut for `create("table", ...)`.

    :param path: path to table.
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param bool recursive: create the path automatically, \
    ``yt.wrapper.config["yamr_mode"]["create_recursive"]`` by default.
    :param bool ignore_existing: do nothing if path already exists otherwise and option specified,
    otherwise if path exists and option is not specified
    then :class:`YtResponseError <yt.wrapper.errors.YtResponseError>` will be raised.
    :param dict attributes: attributes.
    """

    return _create_table(path, recursive, ignore_existing, attributes, client)

def create_temp_table(path=None, prefix=None, attributes=None, expiration_timeout=None, client=None):
    """Creates temporary table by given path with given prefix and return name.

    :param path: existing path, by default ``yt.wrapper.config["remote_temp_tables_directory"]``.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param str prefix: prefix of table name.
    :param int expiration_timeout: expiration timeout for newly created table, in ms.
    :return: name of result table.
    :rtype: str
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
    expiration_timeout = get_value(expiration_timeout,
                                   get_config(client)["temp_expiration_timeout"])
    timeout = timedelta(milliseconds=expiration_timeout)
    attributes = update(
        {"expiration_time": datetime_to_string(datetime.utcnow() + timeout)},
        get_value(attributes, {}))
    _create_table(name, attributes=attributes, client=client)
    return name

def write_table(table, input_stream, format=None, table_writer=None,
                is_stream_compressed=False, force_create=None, raw=None,
                client=None):
    """Writes rows from input_stream to table.

    :param table: output table. Specify `TablePath` attributes for append mode or something like this. \
    Table can not exist.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param input_stream: python file-like object, string, list of strings.
    :param format: format of input data, ``yt.wrapper.config["tabular_data_format"]`` by default.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param dict table_writer: spec of "write" operation.
    :param bool is_stream_compressed: expect stream to contain compressed table data. \
    This data can be passed directly to proxy without recompression. Be careful! this option \
    disables write retries.
    :param bool force_create: unconditionally creates table and ignores existing table.

    The function tries to split input stream to portions of fixed size and write its with retries.
    If splitting fails, stream is written as is through HTTP.
    Set ``yt.wrapper.config["write_retries"]["enable"]`` to False for writing \
    without splitting and retries.

    Writing is executed under self-pinged transaction.
    """
    if get_config(client)["backend"] == "rpc":
        client = _create_http_client_from_rpc(client, "read_table")

    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]

    if force_create is None:
        force_create = True

    if is_stream_compressed and not raw:
        raise YtError("Compressed stream is only supported for raw tabular data")

    table = TablePath(table, client=client)
    format = _prepare_command_format(format, raw, client)
    table_writer = _prepare_table_writer(table_writer, client)

    params = {}
    params["input_format"] = format.to_yson_type()
    set_param(params, "table_writer", table_writer)

    def prepare_table(path, client):
        if not force_create:
            return
        _create_table(path, ignore_existing=True, client=client)

    can_split_input = isinstance(input_stream, list) or format.is_raw_load_supported()
    enable_retries = get_config(client)["write_retries"]["enable"] and \
        can_split_input and \
        not is_stream_compressed
    if get_config(client)["write_retries"]["enable"] and not can_split_input:
        logger.warning("Cannot split input into rows. Write is processing by one request.")

    enable_parallel_writing = get_config(client)["write_parallel"]["enable"] and \
        can_split_input and \
        not is_stream_compressed and \
        "sorted_by" not in table.attributes

    input_stream = _to_chunk_stream(
        input_stream,
        format,
        raw,
        split_rows=(enable_retries or enable_parallel_writing),
        chunk_size=get_config(client)["write_retries"]["chunk_size"],
        rows_chunk_size=get_config(client)["write_retries"]["rows_chunk_size"])

    if enable_parallel_writing:
        force_create = True
        table = _with_schema_if_exists(table)
        make_parallel_write_request(
            "write_table",
            input_stream,
            table,
            params,
            get_config(client)["write_parallel"]["unordered"],
            prepare_table,
            _get_remote_temp_files_directory(client),
            client=client)
    else:
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

def _prepare_table_path_for_read_blob_table(table, part_index_column_name, client=None):
    table = TablePath(table, client=client)

    sorted_by = get_attribute(table, "sorted_by", client=client)
    if part_index_column_name not in sorted_by:
        raise YtError('Table should be sorted by "{0}"'.format(part_index_column_name))

    table.canonize_exact_ranges()
    required_keys = sorted_by[:sorted_by.index(part_index_column_name)]

    if not table.ranges:
        if required_keys:
            raise YtError("Value for the key {0} must be persented on the path".format(required_keys))

        table.ranges = [{"lower_limit": {"key": [0]}}]
        return table

    if len(table.ranges) > 1:
        raise YtError("Read blob table with multiple ranges is not supported")

    range = table.ranges[-1]
    if "lower_limit" not in range:
        if required_keys:
            raise YtError(
                "Lower limit should consist of columns from the list {0}"
                .format(sorted_by[:len(required_keys)]))
        range["lower_limit"] = {"key": [0]}
        return table

    if "key" not in range["lower_limit"] or len(range["lower_limit"]) != 1:
        raise YtError("Only ranges with keys are supported")

    if len(range["lower_limit"]["key"]) != len(required_keys):
        raise YtError("Key should consist of columns from the list {0}".format(sorted_by[:len(required_keys)]))

    range["lower_limit"]["key"].append(0)
    return table

def read_blob_table(table, part_index_column_name=None, data_column_name=None,
                    part_size=None, table_reader=None, client=None):
    """Reads file from blob table.

    :param table: table to read.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param string part_index_column_name: name of column with part indexes.
    :param string data_column_name: name of column with data.
    :param int part_size: size of each blob.
    :param dict table_reader: spec of "read" operation.
    :rtype: :class:`ResponseStream <yt.wrapper.response_stream.ResponseStream>`.

    """
    if get_config(client)["backend"] == "rpc":
        client = _create_http_client_from_rpc(client, "read_blob_table")

    table = TablePath(table, client=client)

    part_index_column_name = get_value(part_index_column_name, "part_index")
    data_column_name = get_value(data_column_name, "data")

    if part_size is None:
        try:
            part_size = get_attribute(table, "part_size", client=client)
        except YtResponseError as err:
            if err.is_resolve_error():
                raise YtError("You should specify part_size")
            raise

    table = _prepare_table_path_for_read_blob_table(table, part_index_column_name, client)

    params = {
        "path": table,
        "part_index_column_name": part_index_column_name,
        "data_column_name": data_column_name,
        "part_size": part_size
    }
    set_param(params, "table_reader", table_reader)

    def process_response(response):
        pass

    class RetriableState(object):
        def __init__(self):
            self.offset = 0

        def prepare_params_for_retry(self):
            part_index = self.offset // part_size
            offset = self.offset - part_size * part_index

            params["start_part_index"] = part_index
            params["offset"] = offset
            table.attributes["ranges"][0]["lower_limit"]["key"][-1] = part_index
            return params

        def iterate(self, response):
            for chunk in chunk_iter_stream(response, get_config(client)["read_buffer_size"]):
                self.offset += len(chunk)
                yield chunk

    response = make_read_request(
        "read_blob_table",
        table,
        params,
        process_response_action=process_response,
        retriable_state_class=RetriableState,
        client=client)

    return response

def _slice_row_ranges_for_parallel_read(ranges, row_count, data_size, data_size_per_thread):
    result = []
    if row_count > 0:
        row_size = data_size / float(row_count)
    else:
        row_size = 1

    rows_per_thread = max(int(data_size_per_thread / row_size), 1)
    for range in ranges:
        if "exact" in range:
            require("row_index" in range["exact"], lambda: YtError('Invalid YPath: "row_index" not found'))
            lower_limit = range["exact"]["row_index"]
            upper_limit = lower_limit + 1
        else:
            if "lower_limit" in range:
                require("row_index" in range["lower_limit"], lambda: YtError('Invalid YPath: "row_index" not found'))
            if "upper_limit" in range:
                require("row_index" in range["upper_limit"], lambda: YtError('Invalid YPath: "row_index" not found'))

            lower_limit = 0 if "lower_limit" not in range else range["lower_limit"]["row_index"]
            upper_limit = row_count if "upper_limit" not in range else range["upper_limit"]["row_index"]

        for start in xrange(lower_limit, upper_limit, rows_per_thread):
            end = min(start + rows_per_thread, upper_limit)
            result.append((start, end))

    return result

def _prepare_params_for_parallel_read(params, range):
    params["path"].attributes["ranges"] = [{"lower_limit": {"row_index": range[0]},
                                            "upper_limit": {"row_index": range[1]}}]
    return params

def read_table(table, format=None, table_reader=None, control_attributes=None, unordered=None,
               raw=None, response_parameters=None, enable_read_parallel=None, client=None):
    """Reads rows from table and parse (optionally).

    :param table: table to read.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param dict table_reader: spec of "read" operation.
    :param bool raw: don't parse response to rows.
    :rtype: if `raw` is specified -- :class:`ResponseStream <yt.wrapper.response_stream.ResponseStream>`, \
    rows iterator over dicts or :class:`Record <yt.wrapper.yamr_record.Record>` otherwise.

    If ``yt.wrapper.config["read_retries"]["enable"]`` is specified,
    command is executed under self-pinged transaction with retries and snapshot lock on the table.
    This transaction is alive until your finish reading your table, or call `close` method of ResponseStream.
    """
    if get_config(client)["backend"] == "rpc":
        client = _create_http_client_from_rpc(client, "write_table")

    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]

    table = TablePath(table, client=client)
    format = _prepare_command_format(format, raw, client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not exists(table, client=client):
        return ResponseStreamWithReadRow(get_response=lambda: None,
                                         iter_content=iter(EmptyResponseStream()),
                                         close=lambda: None,
                                         process_error=lambda response: None,
                                         get_response_parameters=lambda: None)
    attributes = get(
        table + "/@",
        attributes=["type", "chunk_count", "compressed_data_size", "dynamic", "row_count", "uncompressed_data_size"],
        client=client)

    if attributes.get("type") != "table":
        raise YtError("Command read is supported only for tables")
    if attributes["chunk_count"] > 100 and attributes["compressed_data_size"] // attributes["chunk_count"] < MB:
        logger.info("Table chunks are too small; consider running the following command to improve read performance: "
                    "yt merge --proxy {1} --src {0} --dst {0} --spec '{{combine_chunks=true;}}'"
                    .format(table, get_config(client)["proxy"]["url"]))

    params = {
        "path": table,
        "output_format": format.to_yson_type()
    }
    set_param(params, "table_reader", table_reader)
    set_param(params, "unordered", unordered)

    enable_read_parallel = get_value(enable_read_parallel, get_config(client)["read_parallel"]["enable"])

    if enable_read_parallel:
        if attributes.get("dynamic"):
            logger.warning("Cannot read table in parallel since parallel reading for dynamic tables is not supported")
        elif control_attributes is not None:
            logger.warning('Cannot read table in parallel since parameter "control_attributes" is specified')
        elif table.has_key_limit_in_ranges():
            logger.warning("Cannot read table in parallel since table path contains key limits")
        else:
            if "ranges" not in table.attributes:
                table.attributes["ranges"] = [
                    {"lower_limit": {"row_index": 0},
                     "upper_limit": {"row_index": attributes["row_count"]}}]
            ranges = _slice_row_ranges_for_parallel_read(
                table.attributes["ranges"],
                attributes["row_count"],
                attributes["uncompressed_data_size"],
                get_config(client)["read_parallel"]["data_size_per_thread"])
            response_parameters = get_value(response_parameters, {})
            if not ranges:
                response_parameters["start_row_index"] = 0
                response_parameters["approximate_row_count"] = 0
            else:
                response_parameters["start_row_index"] = ranges[0][0]
                response_parameters["approximate_row_count"] = sum(range[1] - range[0] for range in ranges)
            response = make_read_parallel_request(
                "read_table",
                table,
                ranges,
                params,
                _prepare_params_for_parallel_read,
                unordered,
                response_parameters,
                client)
            if raw:
                return response
            else:
                return format.load_rows(response)

    set_param(params, "control_attributes", control_attributes)

    def set_response_parameters(parameters):
        if response_parameters is not None:
            for key in parameters:
                response_parameters[key] = parameters[key]

    def process_response(response):
        if response.response_parameters is None:
            raise YtIncorrectResponse("X-YT-Response-Parameters missing (bug in proxy)", response._get_response())
        set_response_parameters(response.response_parameters)

    chaos_monkey_enabled = get_option("_ENABLE_READ_TABLE_CHAOS_MONKEY", client)
    chaos_monkey = default_chaos_monkey(chaos_monkey_enabled)

    multiple_ranges_allowed = get_config(client)["read_retries"]["allow_multiple_ranges"]

    class RetriableState(object):
        def __init__(self):
            # Whether reading started, it is used only for reading without ranges in <= 0.17.3 versions.
            self.started = False

            # Row and range indices.
            self.next_row_index = None
            self.current_range_index = 0

            # It is true if and only if we read control attributes of the current range
            # and the next range isn't started yet.
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
            def fix_range(range):
                if "exact" in range:
                    if self.range_started:
                        del range["exact"]
                        range["lower_limit"] = range["upper_limit"] = {"row_index": 0}
                else:
                    range["lower_limit"] = {"row_index": self.next_row_index}

            if "ranges" not in table.attributes:
                if self.started:
                    fix_range(table.attributes)
            else:
                if len(table.attributes["ranges"]) > 1:
                    if multiple_ranges_allowed:
                        if "control_attributes" not in params:
                            params["control_attributes"] = {}
                        params["control_attributes"]["enable_row_index"] = True
                        params["control_attributes"]["enable_range_index"] = True
                    else:
                        raise YtError(
                            "Read table with multiple ranges using retries is disabled, "
                            "turn on read_retries/allow_multiple_ranges")

                    if format.name() not in ["json", "yson"]:
                        raise YtError("Read table with multiple ranges using retries "
                                      "is supported only in YSON and JSON formats")
                    if format.name() == "json" and format.attributes.get("format") == "pretty":
                        raise YtError("Read table with multiple ranges using retries "
                                      "is not supported for pretty JSON format")

                if self.range_started and table.attributes["ranges"]:
                    fix_range(table.attributes["ranges"][0])
                self.range_started = False

            params["path"] = table

            return params

        def iterate(self, response):
            format_for_raw_load = deepcopy(format)
            if isinstance(format_for_raw_load, YsonFormat) and format_for_raw_load.attributes["lazy"]:
                format_for_raw_load.attributes["lazy"] = False

            format_for_raw_load_name = format_for_raw_load.name()

            def is_control_row(row):
                if format_for_raw_load_name == "yson":
                    return row.endswith(b"#;")
                elif format_for_raw_load_name == "json":
                    if b"$value" not in row:
                        return False
                    loaded_row = json.loads(row)
                    return "$value" in loaded_row and loaded_row["$value"] is None
                else:
                    return False

            def load_control_row(row):
                if format_for_raw_load_name == "yson":
                    return next(yson.loads(row, yson_type="list_fragment"))
                elif format_for_raw_load_name == "json":
                    return yson.json_to_yson(json.loads(row))
                else:
                    assert False, "Incorrect format"

            def dump_control_row(row):
                if format_for_raw_load_name == "yson":
                    return yson.dumps([row], yson_type="list_fragment")
                elif format_for_raw_load_name == "json":
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

            for row in format_for_raw_load.load_rows(response, raw=True):
                run_chaos_monkey(chaos_monkey)

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
        return format.load_rows(response)

def _are_valid_nodes(source_tables, destination_table):
    return \
        len(source_tables) == 1 and \
        not source_tables[0].has_delimiters() and \
        not destination_table.append and \
        destination_table != source_tables[0]

def copy_table(source_table, destination_table, replace=True, client=None):
    """Copies table(s).

    :param source_table: source table or list of tables.
    :type source_table: list[str or :class:`TablePath <yt.wrapper.ypath.TablePath>`]
    :param destination_table: destination table.
    :type destination_table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param bool replace: override `destination_table`.

    .. note:: param `replace` is overridden by \
    ``yt.wrapper.config["yamr_mode"]["replace_tables_on_copy_and_move"]``

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
        is_sorted_merge = \
            all(imap(lambda t: is_sorted(t, client=client), source_tables)) \
            and not destination_table.append
        mode = "sorted" if is_sorted_merge else "ordered"
        run_merge(source_tables, destination_table, mode, client=client)

def move_table(source_table, destination_table, replace=True, client=None):
    """Moves table.

    :param source_table: source table or list of tables.
    :type source_table: list[str or :class:`TablePath <yt.wrapper.ypath.TablePath>`]
    :param destination_table: destination table.
    :type destination_table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param bool replace: override `destination_table`.

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

def row_count(table, client=None):
    """Returns number of rows in the table.

    :param table: table.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :rtype: int
    """
    table = TablePath(table, client=client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not exists(table, client=client):
        return 0
    return get_attribute(table, "row_count", client=client)

def is_empty(table, client=None):
    """Is table empty?

    :param table: table.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :rtype: bool
    """
    return apply_function_to_result(
        lambda res: res == 0,
        row_count(TablePath(table, client=client), client=client))

def get_sorted_by(table, default=None, client=None):
    """Returns "sorted_by" table attribute or `default` if attribute doesn't exist.

    :param table: table.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param default: whatever.
    :rtype: str or list[str]
    """
    if default is None:
        default = [] if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] else None
    return get_attribute(TablePath(table, client=client), "sorted_by", default=default, client=client)

def is_sorted(table, client=None):
    """Is table sorted?

    :param table: table.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :rtype: bool
    """
    if get_config(client)["yamr_mode"]["use_yamr_sort_reduce_columns"]:
        return get_sorted_by(table, [], client=client) == ["key", "subkey"]
    else:
        return get_attribute(TablePath(table, client=client),
            "sorted",
            default=False,
            client=client)

def alter_table(path, schema=None, dynamic=None, upstream_replica_id=None, client=None):
    """Performs schema and other table meta information modifications.
       Applicable to static and dynamic tables.

    :param path: path to table
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param schema: new schema to set on table
    :param bool dynamic: dynamic
    :param str upstream_replica_id: upstream_replica_id
    """

    params = {"path": TablePath(path, client=client)}
    set_param(params, "schema", schema)
    set_param(params, "dynamic", dynamic)
    set_param(params, "upstream_replica_id", upstream_replica_id)

    return make_request("alter_table", params, client=client)
