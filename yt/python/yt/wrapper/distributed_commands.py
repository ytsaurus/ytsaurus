from .common import set_param
from .config import get_config
from .driver import make_request, make_formatted_request
from .errors import YtResponseError, YtError
from .stream import ItemStream
from .table_helpers import _to_chunk_stream, _prepare_command_format
from .transaction_commands import ping_transaction
from .ypath import YPath
from .yson import loads, get_bytes

from typing import Union, Optional, Any, Dict, List, Tuple, Iterable, BinaryIO, TypedDict


class DistributedWriteCookePacketType(TypedDict):
    class CookiesPayloadType(TypedDict):
        cookie_id: str
        session_id: str  # same as Session.payload.root_chunk_list_id
        transaction_id: str
        patch_info: Dict[str, Any]

    class CookieHeaderType(TypedDict):
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
    class SessionPayloadType(TypedDict):
        main_transaction_id: str
        root_chunk_list_id: str
        upload_transaction_id: str
        patch_info: Dict[str, Any]

    class SessionHeaderType(DistributedWriteCookePacketType.CookieHeaderType):
        pass

    header: str
    signature: bytes
    payload: bytes


class DistributedWriteFragmentPacketType(TypedDict):
    class FragmentPayloadType(TypedDict):
        cookie_id: str
        session_id: str  # same as Session.payload.root_chunk_list_id
        chunk_list_id: str
        max_boundary_key: List[str]
        min_boundary_key: List[str]

    class FragmentHeaderType(DistributedWriteCookePacketType.CookieHeaderType):
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
