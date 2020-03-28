import yp_proto.yp.client.api.proto.object_service_pb2 as object_service_pb2

import yp.data_model as data_model

from yp.common import (
    YpInvalidContinuationTokenError,
    YtResponseError,
    YpAuthenticationError
)

from yt.yson import YsonEntity

from yt.packages.six.moves import xrange

import pytest


@pytest.mark.usefixtures("yp_env_auth")
class TestObjectService(object):
    def test_select_empty_field(self, yp_env_auth):
        yp_client = yp_env_auth.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set")
        yp_client.create_object(object_type="pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        selection_result = yp_client.select_objects(
            "pod", selectors=["/spec/virtual_service_options/ip4_mtu"]
        )
        assert len(selection_result) == 1 and isinstance(selection_result[0][0], YsonEntity)
        selection_result = yp_client.select_objects(
            "pod", selectors=["/status/agent/iss/currentStates"]
        )
        assert len(selection_result) == 1 and isinstance(selection_result[0][0], YsonEntity)
        selection_result = yp_client.select_objects(
            "pod", selectors=["/status/agent/iss/currentStates/1"]
        )
        assert len(selection_result) == 1 and isinstance(selection_result[0][0], YsonEntity)

    def test_select_nonexistent_field(self, yp_env_auth):
        yp_client = yp_env_auth.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set")
        yp_client.create_object(object_type="pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        with pytest.raises(YtResponseError):
            yp_client.select_objects("pod", selectors=["/status/agent/iss/currentStates/1a"])
        with pytest.raises(YtResponseError):
            yp_client.select_objects("pod", selectors=["/status/agent/iss/nonexistentField"])
        with pytest.raises(YtResponseError):
            yp_client.select_objects(
                "pod", selectors=["/status/agent/iss/currentStates/1/nonexistentField"]
            )
        with pytest.raises(YtResponseError):
            yp_client.select_objects(
                "pod", selectors=["/spec/ip6_address_requests/network_id/network_id"]
            )

    def test_get_nonexistent(self, yp_env_auth):
        yp_client = yp_env_auth.yp_client

        existent_id = "existent_id"
        nonexistent_id = "nonexistent_id"
        assert (
            yp_client.create_object("pod_set", attributes={"meta": {"id": existent_id}})
            == existent_id
        )

        with pytest.raises(YtResponseError):
            yp_client.get_objects("pod_set", [existent_id, nonexistent_id], selectors=["/meta/id"])

        with pytest.raises(YtResponseError):
            yp_client.get_object("pod_set", nonexistent_id, selectors=["/meta/id"])

        objects = yp_client.get_objects(
            "pod_set",
            [existent_id, nonexistent_id],
            selectors=["/meta/id"],
            options={"ignore_nonexistent": True},
        )
        assert len(objects) == 2
        assert len(objects[0]) == 1 and objects[0][0] == existent_id
        assert objects[1] is None

        assert (
            yp_client.get_object(
                "pod_set",
                nonexistent_id,
                selectors=["/meta/id"],
                options={"ignore_nonexistent": True},
            )
            is None
        )

    def test_create_object_null_attributes_payload(self, yp_env_auth):
        yp_client = yp_env_auth.yp_client

        object_stub = yp_client.create_grpc_object_stub()

        req = object_service_pb2.TReqCreateObject()
        req.object_type = data_model.OT_POD_SET
        req.attributes_payload.null = True

        rsp = object_stub.CreateObject(req)

        assert len(rsp.object_id) > 0

    def test_create_objects_null_attributes_payload(self, yp_env_auth):
        yp_client = yp_env_auth.yp_client

        object_stub = yp_client.create_grpc_object_stub()

        req = object_service_pb2.TReqCreateObjects()
        subreq = req.subrequests.add()
        subreq.object_type = data_model.OT_POD_SET
        subreq.attributes_payload.null = True

        rsp = object_stub.CreateObjects(req)

        assert len(rsp.subresponses[0].object_id) > 0

    def test_select_objects_continuation(self, yp_env_auth):
        yp_client = yp_env_auth.yp_client

        def validate_batches(batches, expected_ids, expected_limit):
            found_ids = set()

            for i in xrange(len(batches)):
                if i > 0 and len(batches[i]) > 0:
                    assert len(batches[i - 1]) > 0
                    assert batches[i][0] > batches[i - 1][-1]

                batch = batches[i]

                for j in xrange(len(batch)):
                    if j > 0:
                        assert batch[j] > batch[j - 1]

                    # NB! In the test /meta/id is always the last field in selector.
                    object_id = batch[j][-1]
                    assert object_id not in found_ids
                    found_ids.add(object_id)

                assert len(batch) <= expected_limit
                if len(batch) < expected_limit:
                    assert i + 1 == len(batches)

            assert expected_ids == found_ids

        object_count = 100

        def impl(object_type, ids, selectors):
            for limit in (11, 5, 3, 1, object_count + 3):
                timestamp = yp_client.generate_timestamp()

                id_upper_bound = "scheduling"
                batches = []

                continuation_token = None
                while True:
                    options = dict(limit=limit)
                    if continuation_token is not None:
                        options["continuation_token"] = continuation_token

                    response = yp_client.select_objects(
                        object_type,
                        filter='[/meta/id] < "{}"'.format(id_upper_bound),
                        selectors=selectors,
                        options=options,
                        enable_structured_response=True,
                        timestamp=timestamp,
                    )

                    continuation_token = response["continuation_token"]

                    def subresponse_values(subresponse):
                        return list(
                            map(lambda subresponse_field: subresponse_field["value"], subresponse)
                        )

                    batches.append(list(map(subresponse_values, response["results"])))

                    if len(batches[-1]) < limit:
                        break

                expected_ids = set(filter(lambda object_id: object_id < id_upper_bound, ids))
                assert len(expected_ids) > 0
                validate_batches(batches, expected_ids, limit)

        # Pod set has simple db key.
        ids = []
        for _ in xrange(object_count):
            ids.append(yp_client.create_object("pod_set"))
        impl("pod_set", ids, ["/meta/id"])

        # Pod has more complex db key.
        ids = []
        pod_set_id = yp_client.create_object("pod_set")
        for _ in xrange(object_count):
            ids.append(
                yp_client.create_object("pod", attributes=dict(meta=dict(pod_set_id=pod_set_id)),)
            )
        impl("pod", ids, ["/meta/pod_set_id", "/meta/id"])

    def test_select_objects_continuation_token_of_empty_response(self, yp_env_auth):
        yp_client = yp_env_auth.yp_client

        def select(**options):
            return yp_client.select_objects(
                "pod_set", selectors=["/meta/id"], options=options, enable_structured_response=True,
            )

        response = select(limit=1)
        assert len(response["results"]) == 0
        continuation_token = response["continuation_token"]
        assert len(continuation_token) > 0

        response = select(continuation_token=continuation_token)
        assert len(response["results"]) == 0
        assert continuation_token == response["continuation_token"]

        ids = []
        for _ in xrange(10):
            ids.append(yp_client.create_object("pod_set"))

        response = select(continuation_token=continuation_token)
        assert set(ids) == set(
            map(lambda subresponse: subresponse[0]["value"], response["results"])
        )
        assert continuation_token != response["continuation_token"]
        continuation_token = response["continuation_token"]
        assert len(continuation_token) > 0

        response = select(continuation_token=continuation_token)
        assert len(response["results"]) == 0
        assert continuation_token == response["continuation_token"]

    def test_select_objects_continuation_token_and_offset(self, yp_env_auth):
        yp_client = yp_env_auth.yp_client

        continuation_token = yp_client.select_objects(
            "pod_set",
            selectors=["/meta/id"],
            options=dict(limit=1),
            enable_structured_response=True,
        )["continuation_token"]

        # Works without offset.
        yp_client.select_objects(
            "pod_set", selectors=["/meta/id"], options=dict(continuation_token=continuation_token),
        )

        with pytest.raises(YtResponseError):
            yp_client.select_objects(
                "pod_set",
                selectors=["/meta/id"],
                options=dict(offset=1, continuation_token=continuation_token),
            )

    def test_select_objects_invalid_continuation_token(self, yp_env_auth):
        yp_client = yp_env_auth.yp_client

        with pytest.raises(YpInvalidContinuationTokenError):
            yp_client.select_objects(
                "pod_set", selectors=["/meta/id"], options=dict(continuation_token="abracadabra"),
            )

    def test_select_objects_continuation_token_presence(self, yp_env_auth):
        yp_client = yp_env_auth.yp_client

        def select(options):
            return yp_client.select_objects(
                "account", selectors=["/meta/id"], options=options, enable_structured_response=True,
            )

        for options in (dict(), dict(offset=1), dict(offset=1, limit=10)):
            response = select(options)
            assert "continuation_token" not in response, "Options {}".format(options)

        for options in (dict(limit=100500), dict(limit=1)):
            response = select(options)
            assert len(response["continuation_token"]) > 0, "Options {}".format(options)


# NB: delete after YP-1956 is closed
@pytest.mark.usefixtures("yp_env_configurable")
class TestSupplementaryNoAuth(object):
    YP_MASTER_CONFIG = {
        "object_service": {
            "require_authentication_in_supplementary_calls": False
        },
    }

    def test_call(self, yp_env_configurable):
        user_client = yp_env_configurable.yp_instance.create_client(config={"user": "invalid_user"})
        user_client.generate_timestamp()


# NB: delete after YP-1956 is closed
@pytest.mark.usefixtures("yp_env_configurable")
class TestSupplementaryAuth(object):
    YP_MASTER_CONFIG = {
        "object_service": {
            "require_authentication_in_supplementary_calls": True
        },
    }

    def test_call(self, yp_env_configurable):
        user_id = "batman"
        user_client = yp_env_configurable.yp_instance.create_client(config={"user": user_id})
        with pytest.raises(YpAuthenticationError):
            user_client.generate_timestamp()

        yp_env_configurable.yp_client.create_object("user", attributes={
            "meta": {"id": user_id}})

        yp_env_configurable.sync_access_control()
        user_client.generate_timestamp()
