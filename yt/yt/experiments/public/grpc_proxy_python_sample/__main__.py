from __future__ import print_function

from yt.wire_format import (serialize_rows_to_unversioned_wire_format, deserialize_rows_from_unversioned_wire_format,
                            AttachmentStream, SCHEMA_TYPE_TO_VALUE_TYPE)

import yt_proto.yt.client.api.rpc_proxy.proto.api_service_pb2 as api_service_pb2

import yt.yson as yson

import yt.wrapper as yt

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

import time
import random
import grpc

TEST_TABLE_PATH = "//home/asaitgalin/grpc_proxy_example_table"

TEST_TABLE_SCHEMA = [
    {
        "name": "a",
        "type": "int64",
        "sort_order": "ascending"
    },
    {
        "name": "b",
        "type": "string"
    },
    {
        "name": "c",
        "type": "any"
    },
    {
        "name": "d",
        "type": "uint64"
    }
]

TEST_TABLE_ROWS = [
    {"a": 4, "b": "one"},
    {"a": 5, "c": {"two": "three"}},
    {"a": 7, "d": 4},
    {"a": 8, "b": "five", "d": 5}
]

# All available types are listed in EObjectType enum in server code.
STRING_NODE_TYPE_TO_NUMERIC_TYPE = {
    "map_node": 303,
    "list_node": 304,
    "file": 400,
    "table": 401,
    "link": 417,
    "document": 421
}


def _get_body_size(meta):
    for k, v in meta:
        if k == "yt-message-body-size":
            return int(v)

    assert False, "Message body size should be received"


# Sends raw data via GRPC. Returns raw response and trailing metadata.
# Method will be used to send requests / receive responses with attachments.
def make_request(channel, method, data, message_body_size=None):
    if message_body_size is None:
        message_body_size = len(data)

    metadata = [
        # Min required protocol version is "1.0".
        ("yt-protocol-version", "1.0"),
        # YT token.
        ("yt-auth-token", yt._get_token()),
        # Size of serialized protobuf message in the beginning of `data`.
        # It is necessary to send this value for all requests with attachments.
        ("yt-message-body-size", str(message_body_size))
    ]

    unary = channel.unary_unary("/ApiService/" + method, request_serializer=None, response_deserializer=None)
    _, call = unary.with_call(data, metadata=metadata)

    return call.result(), call.trailing_metadata()


# Wrapper around make_request, used for simple requests that does not work with
# attachments.
def make_light_request(channel, method, params):
    req_class = getattr(api_service_pb2, "TReq" + method)
    rsp_class = getattr(api_service_pb2, "TRsp" + method)

    req = req_class(**params)
    rsp, _ = make_request(channel, method, req.SerializeToString())
    return rsp_class.FromString(rsp)


def main():
    yt.config["proxy"]["url"] = "hume"

    # Retrieve gRPC proxy list via HTTP. Command "discover_proxies" is available only in API v4.
    yt.config["api_version"] = "v4"
    proxies = yt.driver.make_formatted_request("discover_proxies", {"type": "grpc"}, format=None)["proxies"]

    # Create gRPC channel to random proxy.
    channel = grpc.insecure_channel(str(random.choice(proxies)))

    # Create test dynamic table with schema.
    create_req = api_service_pb2.TReqCreateNode(path=TEST_TABLE_PATH, type=STRING_NODE_TYPE_TO_NUMERIC_TYPE["table"])

    attribute = create_req.attributes.attributes.add()
    attribute.key = "dynamic"
    attribute.value = b"%true"

    attribute = create_req.attributes.attributes.add()
    attribute.key = "schema"
    attribute.value = yson.dumps(TEST_TABLE_SCHEMA)

    make_request(channel, "CreateNode", create_req.SerializeToString())

    # Mount test table.
    make_light_request(channel, "MountTable", {"path": TEST_TABLE_PATH})

    # Wait until table is mounted.
    start_time = time.time()

    while time.time() - start_time < 15:
        tablets = yson.loads(make_light_request(channel, "GetNode", {"path": TEST_TABLE_PATH + "/@tablets"}).value)
        if all(tablet["state"] == "mounted" for tablet in tablets):
            break

        time.sleep(0.3)

    # Starting dynamic tables transaction.
    tx = make_light_request(
        channel,
        "StartTransaction",
        {"type": api_service_pb2.ETransactionType.Value("TT_TABLET"), "timeout": 120000, "sticky": True})

    # Filling rowset descriptor from table schema.
    # Rowset descriptor maps column index to column name and vice versa.
    # This descriptor will be used on server during deserialization.
    rowset_desc = api_service_pb2.TRowsetDescriptor()

    for column in TEST_TABLE_SCHEMA:
        desc = rowset_desc.name_table_entries.add()
        desc.name = column["name"]
        desc.type = SCHEMA_TYPE_TO_VALUE_TYPE[column["type"]]

    # Serializing request message and rows to one resulting stream.
    io = BytesIO()
    io.write(api_service_pb2.TReqModifyRows(
        transaction_id=tx.id,
        path=TEST_TABLE_PATH,
        row_modification_types=[api_service_pb2.ERowModificationType.Value("RMT_WRITE")] * len(TEST_TABLE_ROWS),
        rowset_descriptor=rowset_desc).SerializeToString())
    # Remember serialized proto message, this will be passed to gRPC request.
    body_size = io.tell()
    # Serialize rows to wire format.
    serialize_rows_to_unversioned_wire_format(io, TEST_TABLE_ROWS, TEST_TABLE_SCHEMA)
    # Send serialized message and rows.
    make_request(channel, "ModifyRows", io.getvalue(), message_body_size=body_size)

    # Commit written rows.
    make_light_request(channel, "CommitTransaction", {"transaction_id": tx.id})

    # Select rows, check that they are equal to TEST_TABLE_ROWS and output them.
    select_rsp, select_meta = make_request(
        channel,
        "SelectRows",
        api_service_pb2.TReqSelectRows(query="* FROM [{}]".format(TEST_TABLE_PATH)).SerializeToString())

    # Deserializing message and rows from response.
    body_size = _get_body_size(select_meta)
    select_msg = api_service_pb2.TRspSelectRows.FromString(select_rsp[:body_size])
    stream = AttachmentStream(select_rsp, body_size)

    rows = deserialize_rows_from_unversioned_wire_format(
        stream,
        [entry.name for entry in select_msg.rowset_descriptor.name_table_entries])

    assert rows == TEST_TABLE_ROWS
    print(rows)


if __name__ == "__main__":
    main()
