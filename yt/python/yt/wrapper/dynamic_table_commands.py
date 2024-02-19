from __future__ import print_function

from .driver import make_request, make_formatted_request
from .table_helpers import _prepare_command_format, _to_chunk_stream
from .common import set_param, require, is_master_transaction, YtError, get_value
from .config import get_config, get_option, get_command_param, get_backend_type
from .cypress_commands import get
from .default_config import DEFAULT_WRITE_CHUNK_SIZE
from .errors import (
    YtNoSuchService, YtTabletIsInIntermediateState, YtTabletTransactionLockConflict,
    YtNoSuchTablet, YtTabletNotMounted, YtResponseError, YtRowIsBlocked, YtBlockedRowWaitTimeout,
    YtNoSuchCell, YtChunkNotPreloaded, YtNoInSyncReplicas)
from .ypath import TablePath
from .http_helpers import get_retriable_errors
from .transaction import null_transaction_id
from .retries import Retrier, default_chaos_monkey
from .batch_helpers import create_batch_client

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

try:
    from yt.packages.six import iteritems
except ImportError:
    from six import iteritems

import yt.logger as logger

from copy import deepcopy
import time

SYNC_LAST_COMMITED_TIMESTAMP = 0x3fffffffffffff01
ASYNC_LAST_COMMITED_TIMESTAMP = 0x3fffffffffffff04

TABLET_ACTION_KEEPALIVE_PERIOD = 55  # s


def _waiting_for_condition(condition, error_message, check_interval=None, timeout=None, client=None):
    if check_interval is None:
        check_interval = get_config(client)["tablets_check_interval"] / 1000.0
    if timeout is None:
        timeout = get_config(client)["tablets_ready_timeout"] / 1000.0

    start_time = time.time()
    while not condition():
        if time.time() - start_time > timeout:
            raise YtError(error_message)

        time.sleep(check_interval)


def _waiting_for_tablets(path, state, first_tablet_index=None, last_tablet_index=None, client=None):
    if first_tablet_index is not None or last_tablet_index is not None:
        tablet_count = get(path + "/@tablet_count", client=client)
        first_tablet_index = get_value(first_tablet_index, 0)
        last_tablet_index = get_value(last_tablet_index, tablet_count - 1)
        is_tablets_ready = lambda: get(path + "/@tablet_state", client=client) != "transient" and \
            all(tablet["state"] == state for tablet in  # noqa
                get(path + "/@tablets", client=client)[first_tablet_index:last_tablet_index + 1])
    else:
        is_tablets_ready = lambda: get(path + "/@tablet_state", client=client) == state  # noqa

    _waiting_for_condition(is_tablets_ready, "Timed out while waiting for tablets", client=client)


def _waiting_for_tablet_transition(path, client=None):
    is_tablets_ready = lambda: get(path + "/@tablet_state", client=client) != "transient"  # noqa
    _waiting_for_condition(is_tablets_ready, "Timed out while waiting for tablets", client=client)


def _waiting_for_sync_tablet_actions(tablet_action_ids, client=None):
    def wait_func():
        logger.debug("Waiting for tablet actions %s", tablet_action_ids)
        batch_client = create_batch_client(client=client)
        rsps = [batch_client.get("#{}/@".format(action_id)) for action_id in tablet_action_ids]
        batch_client.commit_batch()

        errors = [YtResponseError(rsp.get_error()) for rsp in rsps if not rsp.is_ok()]
        if errors:
            raise YtError("Waiting for tablet actions failed", inner_errors=errors)

        for rsp in rsps:
            attributes = rsp.get_result()
            logger.debug("%s", attributes)
            if attributes["state"] in ("completed", "failed"):
                action_id = attributes["id"]
                logger.info(
                    "Tablet action %s out of %s finished",
                    total_action_count - len(tablet_action_ids) + 1,
                    total_action_count)
                tablet_action_ids.remove(action_id)
                if attributes["state"] == "failed":
                    logger.warning("Tablet action %s failed with error \"%s\"", action_id, attributes["error"])
                    logger.warning("%s", attributes)

        return len(tablet_action_ids) == 0

    if not tablet_action_ids:
        return

    logger.info("Waiting for %s tablet action(s)", len(tablet_action_ids))

    tablet_action_ids = list(tablet_action_ids)
    total_action_count = len(tablet_action_ids)

    _waiting_for_condition(
        wait_func,
        "Tablet actions did not finish in time",
        timeout=TABLET_ACTION_KEEPALIVE_PERIOD,
        check_interval=1.0,
        client=client)


def _check_transaction_type(client):
    transaction_id = get_command_param("transaction_id", client=client)
    if transaction_id == null_transaction_id:
        return
    if get_backend_type(client) in ("native", "rpc"):
        return
    require(not is_master_transaction(transaction_id),
            lambda: YtError("Dynamic table commands can not be performed under master transaction"))


def get_dynamic_table_retriable_errors():
    return tuple(
        list(get_retriable_errors()) + [
            YtNoSuchService,
            YtTabletIsInIntermediateState,
            YtTabletTransactionLockConflict,
            YtNoSuchTablet,
            YtTabletNotMounted,
            YtRowIsBlocked,
            YtBlockedRowWaitTimeout,
            YtNoSuchCell,
            YtChunkNotPreloaded,
            YtNoInSyncReplicas,
        ])


class DynamicTableRequestRetrier(Retrier):
    def __init__(self, retry_config, command, params, return_content=True, data=None, client=None):
        request_timeout = get_config(client)["proxy"]["heavy_request_timeout"]
        chaos_monkey_enable = get_option("_ENABLE_HEAVY_REQUEST_CHAOS_MONKEY", client)
        super(DynamicTableRequestRetrier, self).__init__(
            retry_config=retry_config,
            timeout=request_timeout,
            exceptions=get_dynamic_table_retriable_errors(),
            chaos_monkey=default_chaos_monkey(chaos_monkey_enable))

        self.request_timeout = request_timeout
        self.params = params
        self.command = command
        self.client = client
        self.data = data
        self.return_content = return_content

    def action(self):
        kwargs = {}
        if self.data is not None:
            kwargs["data"] = self.data

        response = make_request(
            self.command,
            self.params,
            return_content=self.return_content,
            use_heavy_proxy=True,
            timeout=self.request_timeout,
            client=self.client,
            **kwargs)

        if response is not None:
            return BytesIO(response) if self.return_content else response

    def except_action(self, error, attempt):
        logger.warning('Request %s failed with error %s',
                       self.command, repr(error))


def select_rows(query, timestamp=None, input_row_limit=None, output_row_limit=None, range_expansion_limit=None,
                fail_on_incomplete_result=None, verbose_logging=None, enable_code_cache=None, max_subqueries=None,
                workload_descriptor=None, allow_full_scan=None, allow_join_without_index=None, format=None, raw=None,
                execution_pool=None, response_parameters=None, retention_timestamp=None, placeholder_values=None,
                use_canonical_null_relations=None, merge_versioned_rows=None, syntax_version=None, client=None):
    """Executes a SQL-like query on dynamic table.

    .. seealso:: `supported features <https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/dyn-query-language>`_

    :param str query: for example \"<columns> [as <alias>], ... from \\[<table>\\] \
                  [where <predicate> [group by <columns> [as <alias>], ...]]\".
    :param int timestamp: timestamp.
    :param format: output format.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool raw: don't parse response to rows.
    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]
    format = _prepare_command_format(format, raw, client)
    params = {
        "query": query,
        "output_format": format.to_yson_type()}
    set_param(params, "timestamp", timestamp)
    set_param(params, "retention_timestamp", retention_timestamp)
    set_param(params, "input_row_limit", input_row_limit)
    set_param(params, "output_row_limit", output_row_limit)
    set_param(params, "range_expansion_limit", range_expansion_limit)
    set_param(params, "fail_on_incomplete_result", fail_on_incomplete_result)
    set_param(params, "verbose_logging", verbose_logging)
    set_param(params, "enable_code_cache", enable_code_cache)
    set_param(params, "max_subqueries", max_subqueries)
    set_param(params, "workload_descriptor", workload_descriptor)
    set_param(params, "allow_full_scan", allow_full_scan)
    set_param(params, "allow_join_without_index", allow_join_without_index)
    set_param(params, "execution_pool", execution_pool)
    set_param(params, "timeout", get_config(client)["proxy"]["heavy_request_timeout"])
    set_param(params, "enable_statistics", response_parameters is not None)
    set_param(params, "placeholder_values", placeholder_values)
    set_param(params, "use_canonical_null_relations", use_canonical_null_relations)
    set_param(params, "merge_versioned_rows", merge_versioned_rows)
    set_param(params, "syntax_version", syntax_version)

    _check_transaction_type(client)

    response = DynamicTableRequestRetrier(
        get_config(client)["dynamic_table_retries"],
        "select_rows",
        params,
        return_content=False,
        client=client).run()

    if response_parameters is not None:
        response_parameters.update(response.response_parameters)

    if raw:
        return response
    else:
        return format.load_rows(response)


def insert_rows(table, input_stream, update=None, aggregate=None, atomicity=None, durability=None,
                require_sync_replica=None, lock_type=None, format=None, raw=None, client=None):
    """Inserts rows from input_stream to dynamic table.

    :param table: output table path.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param input_stream: python file-like object, string, list of strings.
    :param format: format of input data, ``yt.wrapper.config["tabular_data_format"]`` by default.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool raw: if `raw` is specified stream with unparsed records (strings)
        in specified `format` is expected. Otherwise dicts or :class:`Record <yt.wrapper.yamr_record.Record>`
        are expected.
    :param bool require_sync_replica: require sync replica write.
    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]

    table = TablePath(table, client=client)
    format = _prepare_command_format(format, raw, client)

    params = {}
    params["path"] = table
    params["input_format"] = format.to_yson_type()
    set_param(params, "update", update)
    set_param(params, "aggregate", aggregate)
    set_param(params, "atomicity", atomicity)
    set_param(params, "durability", durability)
    set_param(params, "require_sync_replica", require_sync_replica)
    set_param(params, "lock_type", lock_type)

    chunk_size = get_config(client)["write_retries"]["chunk_size"]
    if chunk_size is None:
        chunk_size = DEFAULT_WRITE_CHUNK_SIZE

    input_data = b"".join(_to_chunk_stream(
        input_stream,
        format,
        raw,
        split_rows=False,
        chunk_size=chunk_size,
        rows_chunk_size=get_config(client)["write_retries"]["rows_chunk_size"]))

    retry_config = deepcopy(get_config(client)["dynamic_table_retries"])
    retry_config["enable"] = retry_config["enable"] and \
        not aggregate and get_command_param("transaction_id", client) == null_transaction_id

    _check_transaction_type(client)

    DynamicTableRequestRetrier(
        retry_config,
        "insert_rows",
        params,
        data=input_data,
        client=client).run()


def explain_query(
        query, timestamp=None, input_row_limit=None, output_row_limit=None, range_expansion_limit=None,
        max_subqueries=None, workload_descriptor=None, allow_full_scan=None, allow_join_without_index=None,
        format=None, raw=None, execution_pool=None, retention_timestamp=None, syntax_version=None, client=None):
    """Explains a SQL-like query on dynamic table.

    .. seealso:: `supported features <https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/dyn-query-language>`_

    :param str query: for example \"<columns> [as <alias>], ... from \\[<table>\\] \
                  [where <predicate> [group by <columns> [as <alias>], ...]]\".
    :param int timestamp: timestamp.
    :param format: output format.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool raw: don't parse response to rows.
    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]
    format = _prepare_command_format(format, raw, client)
    params = {
        "query": query,
        "output_format": format.to_yson_type()}
    set_param(params, "timestamp", timestamp)
    set_param(params, "retention_timestamp", retention_timestamp)
    set_param(params, "input_row_limit", input_row_limit)
    set_param(params, "output_row_limit", output_row_limit)
    set_param(params, "range_expansion_limit", range_expansion_limit)
    set_param(params, "max_subqueries", max_subqueries)
    set_param(params, "workload_descriptor", workload_descriptor)
    set_param(params, "allow_full_scan", allow_full_scan)
    set_param(params, "allow_join_without_index", allow_join_without_index)
    set_param(params, "execution_pool", execution_pool)
    set_param(params, "timeout", get_config(client)["proxy"]["heavy_request_timeout"])
    set_param(params, "syntax_version", syntax_version)

    _check_transaction_type(client)

    response = DynamicTableRequestRetrier(
        get_config(client)["dynamic_table_retries"],
        "explain_query",
        params,
        client=client).run()

    if raw:
        return response
    else:
        return format.load_rows(response)


def delete_rows(table, input_stream, atomicity=None, durability=None, format=None, raw=None,
                require_sync_replica=None, client=None):
    """Deletes rows with keys from input_stream from dynamic table.

    :param table: table to remove rows from.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param input_stream: python file-like object, string, list of strings.
    :param format: format of input data, ``yt.wrapper.config["tabular_data_format"]`` by default.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool raw: if `raw` is specified stream with unparsed records (strings)
        in specified `format` is expected. Otherwise dicts or :class:`Record <yt.wrapper.yamr_record.Record>`
        are expected.
    :param bool require_sync_replica: require sync replica write.
    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]

    table = TablePath(table, client=client)
    format = _prepare_command_format(format, raw, client)

    params = {}
    params["path"] = table
    params["input_format"] = format.to_yson_type()
    set_param(params, "atomicity", atomicity)
    set_param(params, "durability", durability)
    set_param(params, "require_sync_replica", require_sync_replica)

    chunk_size = get_config(client)["write_retries"]["chunk_size"]
    if chunk_size is None:
        chunk_size = DEFAULT_WRITE_CHUNK_SIZE

    input_data = b"".join(_to_chunk_stream(
        input_stream,
        format,
        raw,
        split_rows=False,
        chunk_size=chunk_size,
        rows_chunk_size=get_config(client)["write_retries"]["rows_chunk_size"]))

    retry_config = deepcopy(get_config(client)["dynamic_table_retries"])
    retry_config["enable"] = retry_config["enable"] and \
        get_command_param("transaction_id", client) == null_transaction_id

    _check_transaction_type(client)

    DynamicTableRequestRetrier(
        retry_config,
        "delete_rows",
        params,
        data=input_data,
        client=client).run()


def lock_rows(table, input_stream, locks=[], lock_type=None, durability=None, format=None, raw=None, client=None):
    """Lock rows with keys from input_stream from dynamic table.

    :param table: table to remove rows from.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param input_stream: python file-like object, string, list of strings.
    :param format: format of input data, ``yt.wrapper.config["tabular_data_format"]`` by default.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool raw: if `raw` is specified stream with unparsed records (strings)
        in specified `format` is expected. Otherwise dicts or :class:`Record <yt.wrapper.yamr_record.Record>`
        are expected.
    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]

    table = TablePath(table, client=client)
    format = _prepare_command_format(format, raw, client)

    params = {}
    params["path"] = table
    params["input_format"] = format.to_yson_type()

    set_param(params, "locks", locks)
    set_param(params, "lock_type", lock_type)
    set_param(params, "durability", durability)

    input_data = b"".join(_to_chunk_stream(
        input_stream,
        format,
        raw,
        split_rows=False,
        chunk_size=get_config(client)["write_retries"]["chunk_size"],
        rows_chunk_size=get_config(client)["write_retries"]["rows_chunk_size"]))

    retry_config = deepcopy(get_config(client)["dynamic_table_retries"])
    retry_config["enable"] = retry_config["enable"] and \
        get_command_param("transaction_id", client) == null_transaction_id

    _check_transaction_type(client)

    DynamicTableRequestRetrier(
        retry_config,
        "lock_rows",
        params,
        data=input_data,
        client=client).run()


def lookup_rows(table, input_stream, timestamp=None, column_names=None, keep_missing_rows=None,
                enable_partial_result=None, use_lookup_cache=None,
                format=None, raw=None, versioned=None, retention_timestamp=None, client=None):
    """Lookups rows in dynamic table.

    .. seealso:: `supported features <https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/dyn-query-language>`_

    :param format: output format.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool raw: don't parse response to rows.
    :param bool versioned: return all versions of the requested rows.
    """
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]

    table = TablePath(table, client=client)
    format = _prepare_command_format(format, raw, client)

    params = {}
    params["path"] = table
    params["input_format"] = format.to_yson_type()
    params["output_format"] = format.to_yson_type()
    set_param(params, "timestamp", timestamp)
    set_param(params, "retention_timestamp", retention_timestamp)
    set_param(params, "column_names", column_names)
    set_param(params, "keep_missing_rows", keep_missing_rows)
    set_param(params, "enable_partial_result", enable_partial_result)
    set_param(params, "use_lookup_cache", use_lookup_cache)
    set_param(params, "versioned", versioned)
    set_param(params, "timeout", get_config(client)["proxy"]["heavy_request_timeout"])

    chunk_size = get_config(client)["write_retries"]["chunk_size"]
    if chunk_size is None:
        chunk_size = DEFAULT_WRITE_CHUNK_SIZE

    input_data = b"".join(_to_chunk_stream(
        input_stream,
        format,
        raw,
        split_rows=False,
        chunk_size=chunk_size,
        rows_chunk_size=get_config(client)["write_retries"]["rows_chunk_size"]))

    _check_transaction_type(client)

    response = DynamicTableRequestRetrier(
        get_config(client)["dynamic_table_retries"],
        "lookup_rows",
        params,
        data=input_data,
        client=client).run()

    if raw:
        return response
    else:
        return format.load_rows(response)


def mount_table(path, first_tablet_index=None, last_tablet_index=None, cell_id=None,
                freeze=False, sync=False, target_cell_ids=None, client=None):
    """Mounts the table.

    :param path: path to table.
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param int first_tablet_index: first tablet index.
    :param int last_tablet_index: last tablet index, inclusive.
    :param str cell_id: the id of the cell where all tablets should be mounted to.
    :param bool freeze: whether the table should be mounted in frozen mode.
    :param bool sync: wait for completion.
    :param target_cell_ids:
        the ids of the cells where corresponding tablets should be mounted to.
    """

    if sync and get_option("_client_type", client) == "batch":
        raise YtError("Sync mode is not available with batch client")

    path = TablePath(path, client=client)
    params = {"path": path}
    set_param(params, "first_tablet_index", first_tablet_index)
    set_param(params, "last_tablet_index", last_tablet_index)
    set_param(params, "cell_id", cell_id)
    set_param(params, "target_cell_ids", target_cell_ids)
    set_param(params, "freeze", freeze)

    response = make_request("mount_table", params, client=client)

    if sync:
        state = "frozen" if freeze else "mounted"
        _waiting_for_tablets(path, state, first_tablet_index, last_tablet_index, client)

    return response


def unmount_table(path, first_tablet_index=None, last_tablet_index=None, force=None, sync=False, client=None):
    """Unmounts the table.

    :param path: path to table.
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param int first_tablet_index: first tablet index.
    :param int last_tablet_index: last tablet index, inclusive.
    :param bool force:
        unmounts the table immediately without flushing the dynamic stores.
        May cause data corruption.
    :param bool sync: wait for completion.
    """

    if sync and get_option("_client_type", client) == "batch":
        raise YtError("Sync mode is not available with batch client")

    params = {"path": TablePath(path, client=client)}
    set_param(params, "first_tablet_index", first_tablet_index)
    set_param(params, "last_tablet_index", last_tablet_index)
    set_param(params, "force", force)

    response = make_request("unmount_table", params, client=client)

    if sync:
        _waiting_for_tablets(path, "unmounted", first_tablet_index, last_tablet_index, client)

    return response


def remount_table(path, first_tablet_index=None, last_tablet_index=None, client=None):
    """Remounts the table.

    :param path: path to table.
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param int first_tablet_index: first tablet index.
    :param int last_tablet_index: last tablet index, inclusive.

    This command effectively sends updated table settings to tablets and should be used
    whenever some attributes are set to a mounted table. It is not equivalent to
    unmount+mount and does not cause any downtime.
    """

    params = {"path": path}
    set_param(params, "first_tablet_index", first_tablet_index)
    set_param(params, "last_tablet_index", last_tablet_index)

    return make_request("remount_table", params, client=client)


def freeze_table(path, first_tablet_index=None, last_tablet_index=None, sync=False, client=None):
    """Freezes the table.

    :param path: path to table.
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param int first_tablet_index: first tablet index.
    :param int last_tablet_index: last tablet index, inclusive.
    :param bool sync: wait for completion.
    """

    if sync and get_option("_client_type", client) == "batch":
        raise YtError("Sync mode is not available with batch client")

    params = {"path": TablePath(path, client=client)}
    set_param(params, "first_tablet_index", first_tablet_index)
    set_param(params, "last_tablet_index", last_tablet_index)

    response = make_request("freeze_table", params, client=client)

    if sync:
        _waiting_for_tablets(path, "frozen", first_tablet_index, last_tablet_index, client)

    return response


def unfreeze_table(path, first_tablet_index=None, last_tablet_index=None, sync=False, client=None):
    """Unfreezes the table.

    :param path: path to table.
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param int first_tablet_index: first tablet index.
    :param int last_tablet_index: last tablet index, inclusive.
    :param bool sync: wait for completion.
    """

    if sync and get_option("_client_type", client) == "batch":
        raise YtError("Sync mode is not available with batch client")

    params = {"path": TablePath(path, client=client)}
    set_param(params, "first_tablet_index", first_tablet_index)
    set_param(params, "last_tablet_index", last_tablet_index)

    response = make_request("unfreeze_table", params, client=client)

    if sync:
        _waiting_for_tablets(path, "mounted", first_tablet_index, last_tablet_index, client)

    return response


def reshard_table(path,
                  pivot_keys=None, tablet_count=None, first_tablet_index=None, last_tablet_index=None,
                  uniform=None, enable_slicing=None, slicing_accuracy=None, sync=False, client=None):
    """Changes pivot keys separating tablets of a given table.

    :param path: path to table.
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param int first_tablet_index: first tablet index.
    :param int last_tablet_index: last tablet index, inclusive.
    :param pivot_keys: explicit pivot keys for the new tablets.
    :param int tablet_count:
        desired tablet count used by the system to determine pivot keys
        automatically.
    :param bool uniform:
        pick pivot keys uniformly for given tablet_count. First key column
        must have integral type.
    :param bool enable_slicing:
        use more precise algorithm for picking pivot keys when tablet_count
        is specified.
    :param bool sync: wait for completion.

    """

    if sync and get_option("_client_type", client) == "batch":
        raise YtError("Sync mode is not available with batch client")

    params = {"path": TablePath(path, client=client)}

    set_param(params, "pivot_keys", pivot_keys)
    set_param(params, "tablet_count", tablet_count)
    set_param(params, "first_tablet_index", first_tablet_index)
    set_param(params, "last_tablet_index", last_tablet_index)
    set_param(params, "uniform", uniform)
    set_param(params, "enable_slicing", enable_slicing)
    set_param(params, "slicing_accuracy", slicing_accuracy)

    response = make_request("reshard_table", params, client=client)

    if sync:
        _waiting_for_tablet_transition(path, client)

    return response


def reshard_table_automatic(path, sync=False, client=None):
    """Automatically balance tablets of a mounted table according to tablet balancer config.

    Only mounted tablets will be resharded.

    :param path: path to table.
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param bool sync: wait for the command to finish.
    """

    if sync and get_option("_client_type", client) == "batch":
        raise YtError("Sync mode is not available with batch client")

    params = {"path": TablePath(path, client=client)}

    set_param(params, "keep_actions", sync)

    tablet_action_ids = make_formatted_request("reshard_table_automatic", params, format=None, client=client)
    if sync:
        _waiting_for_sync_tablet_actions(tablet_action_ids, client=client)

    return tablet_action_ids


def balance_tablet_cells(bundle, tables=None, sync=False, client=None):
    """Reassign tablets evenly among tablet cells.

    :param str bundle: tablet cell bundle name.
    :param list tables: if None, all tablets of bundle will be moved. If specified,
        only tablets of `tables` will be moved.
    :param bool sync: wait for the command to finish.
    """

    if sync and get_option("_client_type", client) == "batch":
        raise YtError("Sync mode is not available with batch client")

    params = {"bundle": bundle}

    set_param(params, "tables", tables)
    set_param(params, "keep_actions", sync)

    tablet_action_ids = make_formatted_request("balance_tablet_cells", params, format=None, client=client)
    if sync:
        _waiting_for_sync_tablet_actions(tablet_action_ids, client=client)

    return tablet_action_ids


def trim_rows(path, tablet_index, trimmed_row_count, client=None):
    """Trim rows of the dynamic table.

    :param path: path to table.
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param int tablet_index: tablet index.
    :param int trimmed_row_count: trimmed row count.
    """

    params = {"path": TablePath(path, client=client)}

    set_param(params, "tablet_index", tablet_index)
    set_param(params, "trimmed_row_count", trimmed_row_count)

    return make_request("trim_rows", params, client=client)


def alter_table_replica(replica_id, enabled=None, mode=None, client=None):
    """Changes mode and enables or disables a table replica.

    :param str replica_id: replica id.
    :param bool enabled: enable or disable the replica.
    :param str mode: switch the replica to sync or async mode.
    """

    if mode is not None:
        require(mode in ("sync", "async"), lambda: YtError("Invalid mode. Expected sync or async"))

    params = {"replica_id": replica_id}
    set_param(params, "mode", mode)
    set_param(params, "enabled", enabled)

    return make_request("alter_table_replica", params, client=client)


def get_in_sync_replicas(path, timestamp, input_stream, all_keys=False, cached_sync_replicas_timeout=None,
                         format=None, raw=None, client=None):
    """Returns ids of in-sync replicas for keys in input_stream.

    :param path: path to table.
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param timestamp: timestamp replicas are in-sync to.
    :param input_stream: keys subset that must be in-sync.
        Should be python file-like object, string, list of strings.
    :param bool all_keys: ignore input_stream and return in-sync for all keys.
    :param cached_sync_replicas_timeout: the period in seconds.
        Allows to use data from sync replicas cache if the data is no older than cached_sync_replicas_timeout.
    :param format: format of input data.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool raw: if `raw` is specified stream with unparsed records (strings)
        in specified `format` is expected. Otherwise dicts or :class:`Record <yt.wrapper.yamr_record.Record>`
        are expected.
    """

    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]
    format = _prepare_command_format(format, raw, client)

    params = {
        "path": path,
        "timestamp": timestamp,
        "all_keys": all_keys,
        "cached_sync_replicas_timeout": cached_sync_replicas_timeout,
        "input_format": format,
    }

    chunk_size = get_config(client)["write_retries"]["chunk_size"]
    if chunk_size is None:
        chunk_size = DEFAULT_WRITE_CHUNK_SIZE

    input_data = b"".join(_to_chunk_stream(
        input_stream,
        format,
        raw=raw,
        split_rows=False,
        chunk_size=chunk_size,
        rows_chunk_size=get_config(client)["write_retries"]["rows_chunk_size"]))

    return make_formatted_request("get_in_sync_replicas", params, data=input_data, format=None, client=client)


def get_tablet_infos(path, tablet_indexes, format=None, client=None):
    """Returns various runtime tablet information.

    :param path: path to table.
    :type path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param indexes: tablet indexes.
    """

    params = {"path": path, "tablet_indexes": tablet_indexes}
    return make_formatted_request("get_tablet_infos", params, format=format, client=client)


def get_tablet_errors(path, limit=None, format=None, client=None):
    """Returns dynamic table tablet and replication errors.

    :param str path: path to table.
    :param int limit: maximum number of returned errors of any kind.
    """

    params = {"path": path}
    set_param(params, "limit", limit)

    return make_formatted_request("get_tablet_errors", params, format=format, client=client)


class TableBackupManifest(object):
    def __init__(self, source_path, destination_path, ordered_mode=None):
        self.source_path = source_path
        self.destination_path = destination_path
        self.ordered_mode = ordered_mode

    def _serialize(self):
        result = {
            "source_path": self.source_path,
            "destination_path": self.destination_path,
        }
        if self.ordered_mode is not None:
            result["ordered_mode"] = self.ordered_mode
        return result


class ClusterBackupManifest(object):
    def __init__(self):
        self.tables = []

    def add_table(self, source_path, destination_path, ordered_mode=None):
        self._add_table_manifest(
            TableBackupManifest(source_path, destination_path, ordered_mode),
        )
        return self

    def merge(self, other):
        assert isinstance(other, ClusterBackupManifest)

        for table_manifest in other.tables:
            self._add_table_manifest(deepcopy(table_manifest))

        return self

    def _add_table_manifest(self, table_manifest):
        self.tables.append(table_manifest)

    def _serialize(self):
        return [table._serialize() for table in self.tables]


class BackupManifest(object):
    def __init__(self):
        self.clusters = {}

    def add_cluster(self, cluster_name, cluster_manifest):
        assert cluster_name not in self.clusters

        self.clusters[cluster_name] = cluster_manifest
        return self

    def update_cluster(self, cluster_name, cluster_manifest):
        self.clusters.setdefault(
            cluster_name, ClusterBackupManifest(),
        ).merge(cluster_manifest)
        return self

    def merge(self, other):
        assert isinstance(other, BackupManifest)

        for cluster_name, cluster_manifest in iteritems(other.clusters):
            self.update_cluster(cluster_name, cluster_manifest)
        return self

    def _serialize(self):
        clusters = {
            name: cluster._serialize()
            for name, cluster
            in iteritems(self.clusters)
        }
        return {"clusters": clusters}


def create_table_backup(
        manifest, force=None, checkpoint_timestamp_delay=None,
        checkpoint_check_timeout=None, preserve_account=None, client=None):
    """Creates a consistent backup copy of a collection of tables.

    :param manifest: description of tables to be backed up.
    :type manifest: dict or :class:`BackupManifest`
    :param bool force: overwrite destination tables.
    """
    if isinstance(manifest, BackupManifest):
        manifest = manifest._serialize()

    params = {"manifest": manifest}
    set_param(params, "force", force)
    set_param(params, "checkpoint_timestamp_delay", checkpoint_timestamp_delay)
    set_param(params, "checkpoint_check_timeout", checkpoint_check_timeout)
    set_param(params, "preserve_account", preserve_account)

    return make_request("create_table_backup", params, client=client)


def restore_table_backup(
        manifest, force=None, mount=None, enable_replicas=None,
        preserve_account=None, client=None):
    """Restores a collection of tables from its backup copy.

    :param manifest: description of tables to be restored.
    :type manifest: dict or :class:`BackupManifest`
    :param bool force: overwrite destination tables.
    :param bool mount: mount restored tables which were mounted before backup.
    :param bool enable_replicas: enable restored table replicas which were enabled before backup.
    """
    if isinstance(manifest, BackupManifest):
        manifest = manifest._serialize()

    params = {"manifest": manifest}
    set_param(params, "force", force)
    set_param(params, "mount", mount)
    set_param(params, "enable_replicas", enable_replicas)
    set_param(params, "preserve_account", preserve_account)

    return make_request("restore_table_backup", params, client=client)
