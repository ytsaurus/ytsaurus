from yt_env_setup import YTEnvSetup, wait
# NOTE(asaitgalin): No yt_commands import here, only rpc api should be used! :)

from yt_yson_bindings import loads_proto, dumps_proto, loads, dumps

import yt_proto.yt.ytlib.rpc_proxy.proto.api_service_pb2 as api_service_pb2

from yt.environment.helpers import assert_items_equal
from yt.common import YtError, guid_to_parts, parts_to_guid, underscore_case_to_camel_case

import grpc
import sys
import struct
from datetime import datetime
from cStringIO import StringIO

SERIALIZATION_ALIGNMENT = 8

def guid_from_dict(d):
    return parts_to_guid(d["first"], d["second"])

def guid_to_dict(guid):
    parts = guid_to_parts(guid)
    return {"first": parts[0], "second": parts[1]}

def align_up(size):
    return (size + SERIALIZATION_ALIGNMENT - 1) & ~(SERIALIZATION_ALIGNMENT - 1)

class UnversionedWireFormat(object):
    VT_NULL    = 0x02
    VT_INT64   = 0x03
    VT_UINT64  = 0x04
    VT_DOUBLE  = 0x05
    VT_BOOLEAN = 0x06
    VT_STRING  = 0x10
    VT_ANY     = 0x11

    SCHEMA_TYPE_TO_VALUE_TYPE = {
        "int64": VT_INT64,
        "uint64": VT_UINT64,
        "double": VT_DOUBLE,
        "boolean": VT_BOOLEAN,
        "string": VT_STRING
    }

    # uint64(-1)
    NULL_ROW_MARKER = 0xFFFFFFFFFFFFFFFF

    def build_rowset_descriptor_from_schema(self, input_schema):
        columns = []

        for entry in input_schema:
            columns.append({
                "name": entry["name"],
                # TODO(asaitgalin): Will value type and type in descriptor always match numerically?
                "type": self.SCHEMA_TYPE_TO_VALUE_TYPE[entry["type"]]
            })

        return {"columns": columns}

    def dump_rows(self, rows, input_schema):
        key_to_type = dict((entry["name"], entry["type"]) for entry in input_schema)
        key_to_index = dict(zip([entry["name"] for entry in input_schema], range(len(input_schema))))
        aggregate_keys = set(entry["key"] for entry in input_schema if entry.get("aggregate", False))

        stream = StringIO()
        stream.write(struct.pack("<Q", len(rows)))

        for row in rows:
            if row is None:
                raise YtError("Row is None, dict expected")

            values = []

            for key, value in row.iteritems():
                expected_value_type = key_to_type.get(key)

                if expected_value_type is None:
                    raise YtError('Column "{}" is not mentioned in schema'.format(key))
                if key in aggregate_keys:
                    raise YtError('Aggregate key "{}" cannot be specified in a row'.format(key))

                if value is None:
                    continue

                if expected_value_type == "any":
                    serialized_value = dumps(value)
                elif expected_value_type == "int64" or expected_value_type == "uint64":
                    self._validate_supported_types((int, long), expected_value_type, value)
                    formatter = "<q" if expected_value_type == "int64" else "<Q"
                    serialized_value = struct.pack(formatter, value)
                elif expected_value_type == "boolean":
                    self._validate_supported_types((bool, int), expected_value_type, value)
                    serialized_value = struct.pack("<Q", int(value))
                elif expected_value_type == "double":
                    self._validate_supported_types((float,), expected_value_type, value)
                    serialized_value = struct.pack("<d", value)
                elif expected_value_type == "string":
                    # TODO(asaitgalin): Validate character range in unicode or force tranform to
                    # some encoding.
                    self._validate_supported_types((str, unicode), expected_value_type, value)
                    serialized_value = value
                else:
                    raise YtError('Incorrect type in schema: "{}"'.format(expected_value_type))

                value_header = struct.pack(
                    "<HBBI",
                    key_to_index[key],
                    self.SCHEMA_TYPE_TO_VALUE_TYPE[expected_value_type],
                    False,  # "aggregate" = False
                    len(serialized_value))

                assert len(value_header) == 8

                serialized_value_sz = len(serialized_value)
                aligned_serialized_value_sz = align_up(serialized_value_sz)

                serialized_value = serialized_value + "\x00" * (aligned_serialized_value_sz - serialized_value_sz)
                assert len(serialized_value) % SERIALIZATION_ALIGNMENT == 0  # Alignment

                values.append(value_header)
                values.append(serialized_value)

            # NOTE(asaitgalin): Some of the values can be None so writing to stream
            # only when all values are processed.
            # NOTE(asaitgalin): Dividing value count by two since each value has header.
            stream.write(struct.pack("<Q", len(values) // 2))
            stream.write("".join(values))

        return stream.getvalue()

    def load_rows(self, attachments, column_names, skip_none_values=True):
        result = []

        io = StringIO(attachments)
        count = struct.unpack("<Q", self._read_checked(io, 8))[0]

        for _ in range(count):
            column_count = struct.unpack("<Q", self._read_checked(io, 8))[0]
            if column_count == self.NULL_ROW_MARKER:
                result.append(None)

            row = {}

            for _ in range(column_count):
                id_, type_, aggregate, length = struct.unpack("<HBBI", self._read_checked(io, 8))
                assert not aggregate

                if type_ == self.VT_ANY:
                    value = loads(self._read_checked(io, length))
                    self._read_checked(io, align_up(length) - length)
                elif type_ == self.VT_STRING:
                    value = self._read_checked(io, length)
                    self._read_checked(io, align_up(length) - length)
                elif type_ == self.VT_INT64:
                    value = struct.unpack("<q", self._read_checked(io, 8))[0]
                elif type_ == self.VT_UINT64:
                    value = struct.unpack("<Q", self._read_checked(io, 8))[0]
                elif type_ == self.VT_BOOLEAN:
                    value = bool(struct.unpack("<Q", self._read_checked(io, 8))[0])
                elif type_ == self.VT_DOUBLE:
                    value = struct.unpack("<d", self._read_checked(io, 8))[0]
                elif type_ == self.VT_NULL:
                    value = None
                else:
                    assert False, "Type " + str(type_) + " is not supported"

                if skip_none_values and value is None:
                    continue

                row[column_names[id_]] = value

            result.append(row)

        return result

    def _validate_supported_types(self, expected_types, expected_type_in_schema, value):
        if not isinstance(value, expected_types):
            raise YtError('Cannot serialize value of type "{}" to "{}"'.format(
                type(value),
                expected_type_in_schema))

    def _read_checked(self, stream, count):
        res = stream.read(count)
        if len(res) < count:
            raise YtError("Unexpected end of stream while loading rows")
        return res

class TestGrpcProxy(YTEnvSetup):
    ENABLE_RPC_PROXY = True
    USE_DYNAMIC_TABLES = True

    @classmethod
    def setup_class(cls):
        super(TestGrpcProxy, cls).setup_class()
        addresses = cls.Env.get_grpc_proxy_addresses()
        cls.channel = grpc.insecure_channel(addresses[0])

    def _wait_response(self, future):
        while True:
            with future._state.condition:
                if future._state.code is not None:
                    break
                future._state.condition.wait(0.1)

    def _make_light_api_request(self, method, params):
        camel_case_method = underscore_case_to_camel_case(method)

        req_msg_class = getattr(api_service_pb2, "TReq" + camel_case_method)
        rsp_msg_class = getattr(api_service_pb2, "TRsp" + camel_case_method)

        unary = self.channel.unary_unary(
            "/ApiService/" + camel_case_method,
            request_serializer=req_msg_class.SerializeToString,
            response_deserializer=rsp_msg_class.FromString)

        print >>sys.stderr
        print >>sys.stderr, str(datetime.now()), method, params

        rsp = unary.future(loads_proto(dumps(params), req_msg_class))
        self._wait_response(rsp)
        return dumps_proto(rsp.result())

    def _make_heavy_api_request(self, method, params, attachments=None):
        camel_case_method = underscore_case_to_camel_case(method)

        req_msg_class = getattr(api_service_pb2, "TReq" + camel_case_method)
        rsp_msg_class = getattr(api_service_pb2, "TRsp" + camel_case_method)

        serialized_message = loads_proto(dumps(params), req_msg_class).SerializeToString()
        metadata = [("yt-message-body-size", str(len(serialized_message)))]

        message_parts = [serialized_message]

        if attachments is not None:
            message_parts.append(struct.pack("<I", len(attachments)))
            message_parts.append(attachments)

        unary = self.channel.unary_unary("/ApiService/" + camel_case_method)
        rsp = unary.future("".join(message_parts), metadata=metadata)

        print >>sys.stderr
        print >>sys.stderr, str(datetime.now()), method, params

        self._wait_response(rsp)

        message_body_size = None
        for metadata in rsp.trailing_metadata():
            if metadata[0] == "yt-message-body-size":
                message_body_size = int(metadata[1])
                break

        result = rsp.result()
        if message_body_size is None:
            message_body_size = len(result)

        rsp_msg = rsp_msg_class.FromString(result[:message_body_size])

        io = StringIO(result)
        io.seek(message_body_size)

        attachment_parts = []

        while True:
            attachment_size_str = io.read(4)
            if not attachment_size_str:
                break

            if len(attachment_size_str) < 4:
                raise YtError("Unexpected end of stream while reading attachment length")

            attachment_size = struct.unpack("<I", attachment_size_str)[0]

            # Empty attachment marker.
            if attachment_size == 0xFFFFFFFF:
                continue

            remaining = attachment_size
            while remaining > 0:
                read = io.read(remaining)
                if not read:
                    raise YtError("Unexpected end of stream while reading attachment")

                remaining -= len(read)
                attachment_parts.append(read)

        return loads(dumps_proto(rsp_msg)), "".join(attachment_parts)

    def _get_node(self, **kwargs):
        return loads(loads(self._make_light_api_request("get_node", kwargs))["value"])

    def _create_node(self, **kwargs):
        return loads(self._make_light_api_request("create_node", kwargs))["node_id"]

    def _create_object(self, **kwargs):
        object_id_parts = loads(self._make_light_api_request("create_object", kwargs))["object_id"]
        return guid_from_dict(object_id_parts)

    def _exists_node(self, **kwargs):
        return loads(self._make_light_api_request("exists_node", kwargs))["exists"]

    def _mount_table(self, **kwargs):
        self._make_light_api_request("mount_table", kwargs)

    def _start_transaction(self, **kwargs):
        id_parts = loads(self._make_light_api_request("start_transaction", kwargs))["id"]
        return guid_from_dict(id_parts)

    def _commit_transaction(self, **kwargs):
        return loads(self._make_light_api_request("commit_transaction", kwargs))

    def test_cypress_commands(self):
        # 700 = "map_node", see ytlib/object_client/public.h
        self._create_node(type=303, path="//tmp/test")
        assert self._exists_node(path="//tmp/test")
        assert self._get_node(path="//tmp/test/@type") == "map_node"

    def _sync_create_cell(self):
        # 700 = "tablet_cell", see ytlib/object_client/public.h
        cell_id = self._create_object(type=700)
        print >>sys.stderr, "Waiting for tablet cell", cell_id, "to become healthy..."

        def check_cell():
            cell = self._get_node(path="//sys/tablet_cells/" + cell_id, attributes={"columns": ["id", "health", "peers"]})
            if cell.attributes["health"] != "good":
                return False

            node = cell.attributes["peers"][0]["address"]
            if not self._exists_node(path="//sys/nodes/{0}/orchid/tablet_cells/{1}".format(node, cell.attributes["id"])):
                return False

            return True

        wait(check_cell)

    def _sync_mount_table(self, path):
        self._mount_table(path=path)
        wait(lambda: all(tablet["state"] == "mounted" for tablet in self._get_node(path=path + "/@tablets")))

    def test_dynamic_table_commands(self):
        self._sync_create_cell()

        table_path = "//tmp/test"

        schema = [
            {"name": "a", "type": "string", "sort_order": "ascending"},
            {"name": "b", "type": "int64"},
            {"name": "c", "type": "uint64"}
        ]

        # 401 = "table", see ytlib/object_client/public.h
        self._create_node(type=401, path=table_path, attributes={"dynamic": True, "schema": schema})
        self._sync_mount_table(table_path)
        # 1 = "tablet", see ETransactionType in proto
        tx = self._start_transaction(type=1, timeout=10000, sticky=True)
        print >>sys.stderr, tx

        rows = [
            {"a": "Look", "b": 1},
            {"a": "Morty", "b": 2},
            {"a": "I", "c": 3},
            {"a": "am", "b": 7},
            {"a": "pickle", "c": 4},
            {"a": "Rick!", "b": 3L}
        ]

        fmt = UnversionedWireFormat()
        serialized_rows = fmt.dump_rows(rows, schema)

        self._make_heavy_api_request(
            "modify_rows",
            {
                "transaction_id": guid_to_dict(tx),
                "path": table_path,
                # 0 = "write", see ERowModificationType in proto
                "row_modification_types": [0] * len(rows),
                "rowset_descriptor": fmt.build_rowset_descriptor_from_schema(schema)
            },
            attachments=serialized_rows)

        self._commit_transaction(transaction_id=guid_to_dict(tx), sticky=True)

        msg, attachments = self._make_heavy_api_request("select_rows", {"query": "* FROM [{}]".format(table_path)})

        selected_rows = fmt.load_rows(attachments, [col["name"] for col in msg["rowset_descriptor"]["columns"]])
        assert_items_equal(selected_rows, rows)
