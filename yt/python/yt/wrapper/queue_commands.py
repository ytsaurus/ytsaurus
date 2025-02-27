from .driver import make_request, make_formatted_request
from .table_helpers import _prepare_command_format, _to_chunk_stream
from .common import set_param
from .config import get_config, get_command_param
from .default_config import DEFAULT_WRITE_CHUNK_SIZE
from .transaction import null_transaction_id
from .ypath import TablePath

from .dynamic_table_commands import DynamicTableRequestRetrier, _check_transaction_type

from copy import deepcopy


def register_queue_consumer(queue_path, consumer_path, vital, partitions=None, client=None):
    """Registers queue consumer.

    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param consumer_path: path to consumer table.
    :type consumer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param bool vital: vital.
    """

    params = {}
    set_param(params, "queue_path", queue_path, lambda path: TablePath(path, client=client))
    set_param(params, "consumer_path", consumer_path, lambda path: TablePath(path, client=client))
    set_param(params, "vital", vital)
    set_param(params, "partitions", partitions)

    return make_request("register_queue_consumer", params, client=client)


def unregister_queue_consumer(queue_path, consumer_path, client=None):
    """Unregisters queue consumer.

    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param consumer_path: path to consumer table.
    :type consumer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    """

    params = {
        "queue_path": TablePath(queue_path, client=client),
        "consumer_path": TablePath(consumer_path, client=client),
    }

    return make_request("unregister_queue_consumer", params, client=client)


def list_queue_consumer_registrations(queue_path=None, consumer_path=None, format=None, client=None):
    """Lists queue consumer registrations.

    :param queue_path: path to queue table.
    :type queue_path: None or str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param consumer_path: path to consumer table.
    :type consumer_path: None or str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    """

    params = {}
    set_param(params, "queue_path", queue_path, lambda path: TablePath(path, client=client))
    set_param(params, "consumer_path", consumer_path, lambda path: TablePath(path, client=client))

    res = make_formatted_request("list_queue_consumer_registrations", params, format, client=client)
    return res


def pull_queue(queue_path, offset, partition_index,
               max_row_count=None, max_data_weight=None,
               replica_consistency=None,
               format=None, raw=None, client=None):
    """Reads rows from a single partition of a queue (i.e. any ordered dynamic table).
    Returns at most max_row_count consecutive rows of a single tablet with row indexes larger than the given offset.

    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param offset: starting row index.
    :type offset: int
    :param partition_index: index of tablet to read from.
    :type partition_index: int
    :param max_row_count: maximum number of rows to read.
    :type max_row_count: int
    :param max_data_weight: a hint for the maximum data weight of the returned batch in bytes.
    :type max_data_weight: int
    :param replica_consistency: requested read consistency for chaos replicas.
    :type replica_consistency: EReplicaConsistency
    :param format: output format.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool raw: don't parse response to rows.
    """

    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]
    format = _prepare_command_format(format, raw, client)

    params = {
        "queue_path": TablePath(queue_path, client=client),
        "output_format": format.to_yson_type(),
    }
    set_param(params, "offset", offset)
    set_param(params, "partition_index", partition_index)
    set_param(params, "max_row_count", max_row_count)
    set_param(params, "max_data_weight", max_data_weight)
    set_param(params, "replica_consistency", replica_consistency)

    response = DynamicTableRequestRetrier(
        get_config(client)["dynamic_table_retries"],
        "pull_queue",
        params,
        return_content=False,
        client=client).run()

    if raw:
        return response
    else:
        return format.load_rows(response)


def _pull_queue_consumer_impl(consumer_path, queue_path, offset, partition_index,
                              max_row_count=None, max_data_weight=None,
                              replica_consistency=None,
                              format=None, raw=None, method_name="pull_consumer", client=None):
    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]
    format = _prepare_command_format(format, raw, client)

    params = {
        "consumer_path": TablePath(consumer_path, client=client),
        "queue_path": TablePath(queue_path, client=client),
        "output_format": format.to_yson_type(),
    }
    if offset is not None:
        set_param(params, "offset", offset)
    set_param(params, "partition_index", partition_index)
    set_param(params, "max_row_count", max_row_count)
    set_param(params, "max_data_weight", max_data_weight)
    set_param(params, "replica_consistency", replica_consistency)

    response = DynamicTableRequestRetrier(
        get_config(client)["dynamic_table_retries"],
        method_name,
        params,
        return_content=False,
        client=client).run()

    if raw:
        return response
    else:
        return format.load_rows(response)


def pull_queue_consumer(consumer_path, queue_path, offset, partition_index,
                        max_row_count=None, max_data_weight=None,
                        replica_consistency=None,
                        format=None, raw=None, client=None):
    """Reads rows from a single partition of a queue (i.e. any ordered dynamic table) with authorization via consumer.
    Returns at most max_row_count consecutive rows of a single tablet with row indexes larger than the given offset.

    :param consumer_path: path to consumer table.
    :type consumer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param offset: starting row index.
    :type offset: int
    :param partition_index: index of tablet to read from.
    :type partition_index: int
    :param max_row_count: maximum number of rows to read.
    :type max_row_count: int
    :param max_data_weight: a hint for the maximum data weight of the returned batch in bytes.
    :type max_data_weight: int
    :param replica_consistency: requested read consistency for chaos replicas.
    :type replica_consistency: EReplicaConsistency
    :param format: output format.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool raw: don't parse response to rows.
    """
    return _pull_queue_consumer_impl(consumer_path, queue_path, offset, partition_index,
                                     max_row_count=max_row_count, max_data_weight=max_data_weight,
                                     replica_consistency=replica_consistency,
                                     format=format, raw=raw, client=client, method_name="pull_queue_consumer")


def pull_consumer(consumer_path, queue_path, offset, partition_index,
                  max_row_count=None, max_data_weight=None,
                  replica_consistency=None,
                  format=None, raw=None, client=None):
    """Reads rows from a single partition of a queue (i.e. any ordered dynamic table) with authorization via consumer.
    Returns at most max_row_count consecutive rows of a single tablet with row indexes larger than the given offset.

    :param consumer_path: path to consumer table.
    :type consumer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param offset: starting row index.
    :type offset: int
    :param partition_index: index of tablet to read from.
    :type partition_index: int
    :param max_row_count: maximum number of rows to read.
    :type max_row_count: int
    :param max_data_weight: a hint for the maximum data weight of the returned batch in bytes.
    :type max_data_weight: int
    :param replica_consistency: requested read consistency for chaos replicas.
    :type replica_consistency: EReplicaConsistency
    :param format: output format.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool raw: don't parse response to rows.
    """
    return _pull_queue_consumer_impl(consumer_path, queue_path, offset, partition_index,
                                     max_row_count=max_row_count, max_data_weight=max_data_weight,
                                     replica_consistency=replica_consistency,
                                     format=format, raw=raw, client=client, method_name="pull_consumer")


def _advance_queue_consumer_impl(consumer_path, queue_path, partition_index, old_offset, new_offset, client_side=True, client=None, method_name="advance_consumer"):
    params = {
        "consumer_path": TablePath(consumer_path, client=client),
        "queue_path": TablePath(queue_path, client=client),
    }
    set_param(params, "partition_index", partition_index)
    set_param(params, "old_offset", old_offset)
    set_param(params, "new_offset", new_offset)
    set_param(params, "client_side", client_side)

    retry_config = deepcopy(get_config(client)["dynamic_table_retries"])
    retry_config["enable"] = \
        retry_config["enable"] and \
        get_command_param("transaction_id", client) == null_transaction_id

    _check_transaction_type(client)

    DynamicTableRequestRetrier(
        retry_config,
        method_name,
        params,
        client=client).run()


def advance_consumer(consumer_path, queue_path, partition_index, old_offset, new_offset, client_side=True, client=None):
    """Advances consumer offset for the given queue.
    If the old offset is specified, the command fails if it is not equal to the current stored offset.

    :param consumer_path: path to consumer table.
    :type consumer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param partition_index: tablet index
    :type partition_index: int
    :param old_offset: expected current offset
    :type old_offset: None or int
    :param new_offset: new offset to set
    :type new_offset: int
    :param client_side: use client-side implementation
    :type client_side: bool
    """
    _advance_queue_consumer_impl(consumer_path, queue_path, partition_index, old_offset, new_offset, client_side=client_side, client=client, method_name="advance_consumer")


def advance_queue_consumer(consumer_path, queue_path, partition_index, old_offset, new_offset, client_side=True, client=None):
    """Advances consumer offset for the given queue.
    If the old offset is specified, the command fails if it is not equal to the current stored offset.

    :param consumer_path: path to consumer table.
    :type consumer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param partition_index: tablet index
    :type partition_index: int
    :param old_offset: expected current offset
    :type old_offset: None or int
    :param new_offset: new offset to set
    :type new_offset: int
    :param client_side: use client-side implementation
    :type client_side: bool
    """
    _advance_queue_consumer_impl(consumer_path, queue_path, partition_index, old_offset, new_offset, client_side=client_side, client=client, method_name="advance_queue_consumer")


def create_queue_producer_session(producer_path, queue_path, session_id, user_meta=None, format=None, client=None):
    """Creates queue producer session.
    :param producer_path: path to queue producer table.
    :type producer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param session_id: identificator of session.
    :type session_id: str
    :param user_meta: arbitrary user meta information.
    """

    params = {}
    set_param(params, "producer_path", producer_path, lambda path: TablePath(path, client=client))
    set_param(params, "queue_path", queue_path, lambda path: TablePath(path, client=client))
    set_param(params, "session_id", session_id)
    set_param(params, "user_meta", user_meta)

    return make_formatted_request("create_queue_producer_session", params, format, client=client)


def remove_queue_producer_session(producer_path, queue_path, session_id, format=None, client=None):
    """Removes queue producer session.
    :param producer_path: path to queue producer table.
    :type producer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param session_id: identificator of session.
    :type session_id: str
    """

    params = {}
    set_param(params, "producer_path", producer_path, lambda path: TablePath(path, client=client))
    set_param(params, "queue_path", queue_path, lambda path: TablePath(path, client=client))
    set_param(params, "session_id", session_id)

    return make_formatted_request("remove_queue_producer_session", params, format, client=client)


def push_queue_producer(producer_path, queue_path, session_id, epoch, input_stream, user_meta=None, input_format=None, raw=None, output_format=None, require_sync_replica=None, client=None):
    """Push rows to queue via queue producer.
    :param producer_path: path to queue producer table.
    :type producer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param session_id: identificator of session.
    :type session_id: str
    :param epoch: epoch number.
    :type epoch: int
    :param require_sync_replica: require sync replica write.
    :type require_sync_replica: bool
    :param input_stream: python file-like object, string, list of strings.
    :param user_meta: arbitrary user meta information.
    :param input_format: format of input data, ``yt.wrapper.config["tabular_data_format"]`` by default.
    :type input_format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param output_format: format of result, ``yt.wrapper.config["structured_data_format"]`` by default.
    :type output_format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool raw: if `raw` is specified stream with unparsed records (strings)
        in specified `format` is expected. Otherwise dicts or :class:`Record <yt.wrapper.yamr_record.Record>`
        are expected.
    """

    input_format = _prepare_command_format(input_format, raw, client)

    params = {}
    params["input_format"] = input_format.to_yson_type()
    set_param(params, "producer_path", producer_path, lambda path: TablePath(path, client=client))
    set_param(params, "queue_path", queue_path, lambda path: TablePath(path, client=client))
    set_param(params, "session_id", session_id)
    set_param(params, "epoch", epoch)
    set_param(params, "user_meta", user_meta)
    set_param(params, "require_sync_replica", require_sync_replica)

    chunk_size = get_config(client)["write_retries"]["chunk_size"]
    if chunk_size is None:
        chunk_size = DEFAULT_WRITE_CHUNK_SIZE

    input_data = b"".join(_to_chunk_stream(
        input_stream,
        input_format,
        raw,
        split_rows=False,
        chunk_size=chunk_size,
        rows_chunk_size=get_config(client)["write_retries"]["rows_chunk_size"]))

    retry_config = deepcopy(get_config(client)["dynamic_table_retries"])
    retry_config["enable"] = retry_config["enable"] and get_command_param("transaction_id", client) == null_transaction_id

    _check_transaction_type(client)

    return DynamicTableRequestRetrier(
        retry_config,
        "push_queue_producer",
        params,
        data=input_data,
        client=client,
        is_formatted_request=True,
        output_format=output_format).run()
