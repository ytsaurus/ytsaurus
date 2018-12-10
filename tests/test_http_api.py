from yp.client import YpClient

from yp_proto.yp.client.api.proto.object_service_pb2 import TReqCreateObject, TRspCreateObject
from yp_proto.yp.client.api.proto.object_type_pb2 import EObjectType

import yt.yson as yson

import yt.packages.requests as requests

import pytest

import json


class CustomSession(requests.Session):
    def __init__(self, *args, **kwargs):
        super(CustomSession, self).__init__(*args, **kwargs)
        self._request_count = 0

    def get_request_count(self):
        return self._request_count

    def request(self, *args, **kwargs):
        self._request_count += 1
        return super(CustomSession, self).request(*args, **kwargs)


@pytest.mark.usefixtures("yp_env")
class TestHttpApi(object):
    def test_error_headers(self, yp_env):
        rsp = requests.get("http://" + yp_env.yp_instance.yp_http_address + "/ObjectService/StartTransaction")
        assert "X-YT-Error" in rsp.headers
        assert "X-YT-Response-Code" in rsp.headers
        assert "X-YT-Response-Message" in rsp.headers

    def test_protobuf(self, yp_env):
        req = TReqCreateObject()
        req.object_type = EObjectType.Value("OT_POD_SET")

        rsp = requests.post("http://" + yp_env.yp_instance.yp_http_address + "/ObjectService/CreateObject",
            data=req.SerializeToString(),
            headers={"Accept": "application/x-protobuf"})

        rsp.raise_for_status()
        assert rsp.headers["Content-Type"] == "application/x-protobuf"

        reply = TRspCreateObject()
        reply.ParseFromString(rsp.content)

    def test_yson(self, yp_env):
        rsp = requests.post("http://" + yp_env.yp_instance.yp_http_address + "/ObjectService/CreateObject",
            data=yson.dumps({
                "object_type": "pod_set",
                "attributes": {
                    "spec": {
                        "antiaffinity_constraints": [
                            {"key": "dc", "max_pods": 10},
                        ]
                    }
                }
            }),
            headers={
                "Content-Type": "application/x-yson",
                "Accept": "application/x-yson",
            })
        rsp.raise_for_status()
        assert rsp.headers["Content-Type"] == "application/x-yson"

        yson.loads(rsp.content)

    def test_json(self, yp_env):
        rsp = requests.post("http://" + yp_env.yp_instance.yp_http_address + "/ObjectService/CreateObject",
            data=json.dumps({
                "object_type": "pod_set",
                "attributes": {
                    "spec": {
                        "antiaffinity_constraints": [
                            {"key": "dc", "max_pods": 10},
                        ]
                    }
                }
            }),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            })
        rsp.raise_for_status()
        assert rsp.headers["Content-Type"] == "application/json"

        json.loads(rsp.text)

    def test_client(self, yp_env):
        client = yp_env.yp_instance.create_client(transport="http")

        ts1 = client.generate_timestamp()
        ts2 = client.generate_timestamp()
        assert ts1 < ts2

        tx_id = client.start_transaction()

        id = client.create_object("pod_set", transaction_id=tx_id)
        client.update_object("pod_set", id, set_updates=[{"path": "/annotations/a", "value": []}], transaction_id=tx_id)
        client.update_object("pod_set", id, set_updates=[{"path": "/annotations/a/end", "value": 1}], transaction_id=tx_id)
        client.update_object("pod_set", id, set_updates=[{"path": "/annotations/a/end", "value": 2}], transaction_id=tx_id)

        client.commit_transaction(tx_id)

        assert client.get_object("pod_set", id, selectors=["/annotations/a"]) == [[1, 2]]

    def test_client_custom_session(self, yp_env):
        session = CustomSession()
        assert session.get_request_count() == 0
        client = yp_env.yp_instance.create_client(transport="http", _http_session=session)
        ts1 = client.generate_timestamp()
        ts2 = client.generate_timestamp()
        assert ts1 < ts2
        assert session.get_request_count() > 0

    def test_client_connect_timeout(self, yp_env):
        def create_client(connect_timeout):
            return yp_env.yp_instance.create_client(
                transport="http",
                config=dict(connect_timeout=connect_timeout)
            )
        client = create_client(0)
        with pytest.raises(requests.ConnectionError):
            client.generate_timestamp()
        client2 = create_client(2000)
        client2.generate_timestamp()
