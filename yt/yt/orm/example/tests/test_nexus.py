from yt.common import YtResponseError

import pytest
import time


@pytest.mark.parametrize(
    "embedded_path",
    ["/status/column_semaphore", "/status/etc_semaphore"],
)
class TestNexus:
    def test_basics(self, example_env, embedded_path):
        nexus_id = example_env.client.create_object("nexus")
        example_env.client.update_object(
            "nexus",
            nexus_id,
            set_updates=[
                {
                    "path": embedded_path + "/spec/budget",
                    "value": 1,
                    "recursive": True,
                }
            ],
        )

        lease_duration_ms = 5000
        example_env.client.update_object(
            "nexus",
            nexus_id,
            set_updates=[
                {
                    "path": "/control/embedded_semaphore/acquire",
                    "value": {
                        "lease_uuid": "test_lease_uuid",
                        "duration": lease_duration_ms,
                        "embedded_path": embedded_path,
                    },
                },
            ],
        )

        leases = example_env.client.get_object(
            "nexus", nexus_id, selectors=[embedded_path + "/status/leases"]
        )[0]

        assert leases["test_lease_uuid"]["last_acquire_time"] > 0
        assert (
            leases["test_lease_uuid"]["expiration_time"]
            == leases["test_lease_uuid"]["last_acquire_time"] + lease_duration_ms
        )

        example_env.client.update_object(
            "nexus",
            nexus_id,
            set_updates=[
                {
                    "path": "/control/embedded_semaphore/ping",
                    "value": {
                        "lease_uuid": "test_lease_uuid",
                        "duration": lease_duration_ms,
                        "embedded_path": embedded_path,
                    },
                },
            ],
        )

        time.sleep(lease_duration_ms / 1000.0)

        # Ping on expired lease should fail.
        with pytest.raises(YtResponseError):
            example_env.client.update_object(
                "nexus",
                nexus_id,
                set_updates=[
                    {
                        "path": "/control/embedded_semaphore/ping",
                        "value": {
                            "lease_uuid": "test_lease_uuid",
                            "duration": lease_duration_ms,
                            "embedded_path": embedded_path,
                        },
                    },
                ],
            )

        # Release should never fail.
        example_env.client.update_object(
            "nexus",
            nexus_id,
            set_updates=[
                {
                    "path": "/control/embedded_semaphore/release",
                    "value": {
                        "lease_uuid": "test_lease_uuid",
                        "embedded_path": embedded_path,
                    },
                }
            ],
        )


class TestMaps:
    def test_get(self, example_env):
        nested_message_value = {"nested_field": "hello maps!"}
        expected_some_map = {"3": "c"}
        expected_some_map_to_message = {"4": nested_message_value}
        expected_nexus_spec = {
            "some_map": expected_some_map,
            "some_map_to_message": expected_some_map_to_message,
            "some_map_to_message_column": expected_some_map_to_message,
        }
        nexus_id = example_env.client.create_object("nexus", {"spec": expected_nexus_spec})
        (
            nexus_spec,
            some_map,
            some_map_to_message,
            some_map_to_message_column,
            v1,
            v2,
        ) = example_env.client.get_object(
            "nexus",
            nexus_id,
            [
                "/spec",
                "/spec/some_map",
                "/spec/some_map_to_message",
                "/spec/some_map_to_message_column",
                "/spec/some_map/3",
                "/spec/some_map_to_message/4",
            ],
        )
        assert nexus_spec == expected_nexus_spec
        assert some_map == expected_some_map
        assert some_map_to_message == expected_some_map_to_message
        assert some_map_to_message_column == expected_some_map_to_message
        assert v1 == "c"
        assert v2 == nested_message_value

    def test_update(self, example_env):
        nested_message_value = {"nested_field": "hello maps!"}
        expected_some_map = {"3": "c"}
        expected_some_map_to_message = {"4": nested_message_value}
        expected_nexus_spec = {
            "some_map": expected_some_map,
            "some_map_to_message": expected_some_map_to_message,
        }
        nexus_id = example_env.client.create_object("nexus", {"spec": expected_nexus_spec})
        expected_some_map["3"] = "d"
        nested_message_value["nested_field"] = "hello map keys!"
        example_env.client.update_object(
            "nexus",
            nexus_id,
            set_updates=[
                {"path": "/spec/some_map/3", "value": "d"},
                {"path": "/spec/some_map_to_message/4", "value": nested_message_value},
            ],
        )
        some_map, some_map_to_message = example_env.client.get_object(
            "nexus",
            nexus_id,
            [
                "/spec/some_map",
                "/spec/some_map_to_message",
            ],
        )
        assert some_map == expected_some_map
        assert some_map_to_message == expected_some_map_to_message

    def test_update_set_map(self, example_env):
        nexus_id = example_env.client.create_object("nexus")
        some_map = {"3": "c", "4": "d", "5": "l"}
        example_env.client.update_object(
            "nexus",
            nexus_id,
            set_updates=[
                {"path": "/spec/some_map", "value": some_map}
            ]
        )
