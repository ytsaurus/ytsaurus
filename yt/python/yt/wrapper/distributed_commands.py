from .batch_response import apply_function_to_result
from .common import set_param, get_value
from .cypress_commands import get
from .config import get_config
from .driver import make_request, make_formatted_request
from .errors import YtResponseError, YtError
from .format import Format
from .stream import ItemStream
from .transaction_commands import ping_transaction
from .ypath import YPath, TablePath, flatten
from .yson import loads, get_bytes

from typing import Union, Optional, Literal, Any, Dict, List, Tuple, Iterable, BinaryIO, TypedDict


class DistributedWriteCookePacketType(TypedDict):
    class _CookiesPayloadType(TypedDict):
        cookie_id: str
        session_id: str  # same as Session.payload.root_chunk_list_id
        transaction_id: str
        patch_info: Dict[str, Any]

    class _CookieHeaderType(TypedDict):
        version: str
        issuer: str
        keypair_id: str
        signature_id: str
        issued_at: str
        expires_at: str
        valid_after: str

    header: str
    signature: bytes
    payload: bytes


class DistributedWriteSessionPacketType(TypedDict):
    class _SessionPayloadType(TypedDict):
        main_transaction_id: str
        root_chunk_list_id: str
        upload_transaction_id: str
        patch_info: Dict[str, Any]

    class SessionHeaderType(DistributedWriteCookePacketType._CookieHeaderType):
        pass

    header: str
    signature: bytes
    payload: bytes


class DistributedWriteFragmentPacketType(TypedDict):
    class _FragmentPayloadType(TypedDict):
        cookie_id: str
        session_id: str  # same as Session.payload.root_chunk_list_id
        chunk_list_id: str
        max_boundary_key: List[str]
        min_boundary_key: List[str]

    class _FragmentHeaderType(DistributedWriteCookePacketType._CookieHeaderType):
        pass

    header: str
    signature: bytes
    payload: bytes


class DistributedWriteStartSessionType(TypedDict):
    cookies: List[DistributedWriteCookePacketType]
    session: DistributedWriteSessionPacketType


def _sanitize_error(ex: YtResponseError):
    # Sanitizing is needed since the serialization of YtResponseError does not properly support binary (non utf-8) data.
    if ex.params and "session" in ex.params and ex.params["session"]:
        for field_to_rescue in "payload", "signature":
            if field_to_rescue in ex.params["session"] and ex.params["session"][field_to_rescue]:
                ex.params["session"][field_to_rescue] = ex.params["session"][field_to_rescue].decode(errors="ignore")
    return ex


def start_distributed_write_session(
    path: Union[str, YPath],
    fragments_count: Optional[int] = None,
    timeout: Optional[int] = None,
    client=None,
) -> Tuple[DistributedWriteSessionPacketType, DistributedWriteCookePacketType]:
    """Start distribution write session
    """
    params = {
        "path": YPath(path, client=client)
    }

    if timeout is None:
        timeout = get_config(client)["distributed_write"]["session_timeout"]

    set_param(params, "cookie_count", fragments_count)
    set_param(params, "timeout", timeout)

    try:
        raw_result = make_formatted_request(
            "start_distributed_write_session",
            params=params,
            format=None,
            client=client
        )
    except YtResponseError as err:
        err = _sanitize_error(err)
        if err.contains_code(1928):
            # 1928 - "Signature generation is unsupported"
            raise YtError("Proxy is not configured for distributed API")
        else:
            raise err

    return raw_result["session"], raw_result["cookies"]


def finish_distributed_write_session(
    session: DistributedWriteSessionPacketType,
    results: List[object],
    client=None,
):
    """Finish distribution write session
    """
    params = {
        "session": session,
        "results": results,
    }

    try:
        return make_request(
            "finish_distributed_write_session",
            params,
            client=client,
        )
    except YtResponseError as err:
        raise _sanitize_error(err)


def write_table_fragment(
    cookie: DistributedWriteSessionPacketType,
    input_stream: Union[str, bytes, Iterable, BinaryIO],
    format: Optional[str] = None,
    raw: Optional[bool] = None,
    max_row_buffer_size: Optional[int] = None,
    client=None,
) -> DistributedWriteFragmentPacketType:
    """Write distribution table fragment
    """

    from .table_helpers import _to_chunk_stream, _prepare_command_format

    format = _prepare_command_format(format, raw, client)

    if not isinstance(input_stream, ItemStream):
        input_stream = _to_chunk_stream(
            input_stream,
            format,
            raw,
            split_rows=False,
            chunk_size=None,
            rows_chunk_size=get_config(client)["write_retries"]["rows_chunk_size"],
        )

    params = {
        "cookie": cookie,
        "input_format": format,
    }

    set_param(params, "max_row_buffer_size", max_row_buffer_size)

    try:
        return make_formatted_request(
            "write_table_fragment",
            params,
            format=None,
            data=input_stream,
            client=client,
        )
    except YtResponseError as err:
        raise _sanitize_error(err)


def ping_distributed_write_session(
    session: DistributedWriteSessionPacketType,
    timeout: Optional[int] = None,
    client=None,
):
    """Ping distribution write session
    """
    # TODO(denvr): YT-25619 use api method "ping_distributed_write_session"

    session_payload: DistributedWriteSessionPacketType.SessionPayload = loads(get_bytes(session["payload"]))
    ping_transaction(session_payload["main_transaction_id"], timeout=timeout, client=client)


class DistributedReadTablePartitionType(TypedDict):
    class _CookieType(TypedDict):
        class _CookieHeaderType(TypedDict):
            version: str
            issuer: str
            keypair_id: str
            signature_id: str
            issued_at: str
            expires_at: str
            valid_after: str

        header: str  # yson str(!) with Header
        signature: bytes
        payload: bytes

    class AggregateStatisticsType(TypedDict):
        chunk_count: int
        data_weight: int
        row_count: int
        value_count: int
        compressed_data_size: int

    table_ranges: List[YPath]
    cookie: Optional[bytes]  # yson str with CookieType
    aggregate_statistics: AggregateStatisticsType


def guess_table_data_weight_per_partition(
    path: Union[str, YPath],
    table_data_weight: int = None,
    table_chunk_count: int = None,
    max_partition_count: Optional[int] = None,
    max_partition_weight: Optional[int] = None,
    client=None,
) -> int:
    """Get table chunk weight based on "chunk count", "max partition weight" or "one chunk per item policy" round to "read_buffer_size" setting
    """
    client_config = get_config(client)

    if not table_data_weight or not table_chunk_count:
        tables_attributes = get(path, attributes=["data_weight", "chunk_count"], client=client)
        table_data_weight = table_data_weight or tables_attributes["data_weight"]
        table_chunk_count = table_chunk_count or tables_attributes["table_chunk_count"]

    if max_partition_count:
        data_weight_per_partition = max(client_config["read_buffer_size"], int(table_data_weight / max_partition_count))
    elif max_partition_weight:
        data_weight_per_partition = max(client_config["read_buffer_size"], max_partition_weight)
    else:
        data_weight_per_partition = max(client_config["read_buffer_size"], int(table_data_weight / max(1, table_chunk_count)))

    return data_weight_per_partition


def partition_tables(
    paths: Union[Union[str, YPath], List[Union[str, YPath]]],
    partition_mode: Optional[Literal["ordered", "sorted", "unordered"]] = "ordered",
    data_weight_per_partition: Optional[int] = None,
    max_partition_count: Optional[int] = None,
    enable_key_guarantee: Optional[bool] = None,
    adjust_data_weight_per_partition: Optional[bool] = None,
    enable_cookies: Optional[bool] = None,
    omit_inaccessible_rows: Optional[bool] = False,
    client=None,
) -> List[DistributedReadTablePartitionType]:
    """Splits tables into a few partitions
    https://ytsaurus.tech/docs/ru/api/commands#partition_tables
    :param paths: paths to tables
    :type paths: list of (str or :class:`TablePath <yt.wrapper.ypath.TablePath>`)
    :param partition_mode: table partitioning mode, one of the ["sorted", "ordered", "unordered"]
    :param data_weight_per_partition: hint for approximate data weight of each output partition
    :param max_partition_count: maximum output partition count
    :param enable_key_guarantee: a key will be placed to a single chunk exactly
    :param adjust_data_weight_per_partition: allow the data weight per partition to exceed data_weight_per_partition when max_partition_count is set
    :param enable_cookies: return cookie for each partition that can be used with `read_table_partition`

    . seealso:: `partition_tables command in the docs <https://ytsaurus.tech/docs/ru/api/commands#partition_tables>`_
    """
    client_config = get_config(client)

    paths = flatten(paths)

    enable_cookies = bool(enable_cookies)

    params = {}
    set_param(params, "paths", list(map(lambda path: TablePath(path, client=client), paths)))
    set_param(params, "partition_mode", partition_mode)
    set_param(params, "data_weight_per_partition", data_weight_per_partition)
    set_param(params, "max_partition_count", max_partition_count)
    set_param(params, "enable_key_guarantee", enable_key_guarantee)
    set_param(params, "adjust_data_weight_per_partition", adjust_data_weight_per_partition)
    set_param(params, "enable_cookies", enable_cookies)
    set_param(params, "omit_inaccessible_rows", get_value(omit_inaccessible_rows, client_config["read_omit_inaccessible_rows"]))
    response = make_formatted_request("partition_tables", params, client=client, format=None)
    partitions = apply_function_to_result(
        lambda response: response["partitions"],
        response)
    return partitions


def read_table_partition(
    cookie: bytes,
    format: Optional[Union[str, Format]] = None,
    raw: bool = None,
    client=None,
):
    """Read table partition by cookie.
    Call `partition_tables` with parameter `enable_cookies=True` for cookie.

    . seealso:: `read_table_partition command in the docs <https://ytsaurus.tech/docs/ru/api/commands#read_table_partition>`_
    """
    from .table_helpers import _prepare_command_format

    format = _prepare_command_format(format, raw, client)
    params = {
        "cookie": cookie,
        "output_format": format,
    }
    raw_data = make_request(
        "read_table_partition",
        params=params,
        return_content=False,
        use_heavy_proxy=True,
        allow_retries=True,
        client=client,
    )
    if raw:
        return raw_data
    else:
        return format.load_rows(raw_data)
