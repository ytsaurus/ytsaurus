from yt.orm.library.common import wait, InvalidObjectSpecError, RemovalForbiddenError
from yt.orm.library.orchid_client import get_leader_fqdn

from yt.test_helpers.profiler import Profiler

import pytest


class TestMotherShip:
    def test_spec_executor_id(self, example_env):
        executor_id = example_env.client.create_object(
            "executor",
            attributes=dict(meta=dict(id=5)),
            request_meta_response=True,
            enable_structured_response=True,
        )["meta"]["id"]

        assert 5 == executor_id
        nexus_key = example_env.client.create_object("nexus", request_meta_response=True)

        mother_ship_key = example_env.client.create_object(
            "mother_ship",
            attributes=dict(meta=dict(parent_key=nexus_key), spec=dict(executor_id=executor_id)),
            request_meta_response=True,
        )

        assert (
            5
            == example_env.client.get_object(
                "mother_ship",
                mother_ship_key,
                selectors=["/spec/executor_id"],
            )[0]
        )

        example_env.client.remove_object(
            "executor",
            dict(id=executor_id),
        )

        assert (
            0
            == example_env.client.get_object(
                "mother_ship",
                mother_ship_key,
                selectors=["/spec/executor_id"],
            )[0]
        )

        example_env.client.remove_object(
            "mother_ship",
            mother_ship_key,
        )

    def test_templar_count_validator(self, example_env):
        nexus_key = example_env.client.create_object("nexus", request_meta_response=True)
        mother_ship_key = example_env.client.create_object(
            "mother_ship",
            attributes={"meta": {"parent_key": nexus_key}},
            request_meta_response=True,
        )
        example_env.client.create_objects(
            (
                ("interceptor", dict(meta=dict(parent_key=mother_ship_key)))
                for _ in range(10)
            ),
        )
        example_env.client.update_object(
            "mother_ship",
            mother_ship_key,
            set_updates=[
                {
                    "path": "/spec/templar_count",
                    "value": 10,
                }
            ],
        )

        with pytest.raises(InvalidObjectSpecError):
            example_env.client.update_object(
                "mother_ship",
                mother_ship_key,
                set_updates=[
                    {
                        "path": "/spec/templar_count",
                        "value": 20,
                    }
                ],
            )


class TestAttributeSensorsValueTransform:
    def test(self, example_env):
        def _collect_unique_values(sensors):
            return set(
                sensor["tags"]["value"] for sensor in sensors
            )
        mother_ship = example_env.create_mother_ship()

        example_env.client.create_objects(
            (
                ("interceptor", {"meta": {"mother_ship_id": int(mother_ship)}})
                for _ in range(1000)
            )
        )

        example_env.client.update_object(
            "mother_ship",
            str(mother_ship),
            set_updates=[{"path": "/spec/templar_count", "value": 271}]
        )
        example_env.client.update_object(
            "mother_ship",
            str(mother_ship),
            set_updates=[{"path": "/spec/templar_count", "value": 998}]
        )

        leader_fqdn = get_leader_fqdn(example_env.yt_client, "//home/example/master", "Example master")
        profiler = Profiler(
            example_env.yt_client,
            "//home/example/master/instances/{}/orchid/sensors".format(leader_fqdn),
            fixed_tags={
                "object_type": "mother_ship",
            },
        )

        wait(lambda: {"swarm", "zounds"}.issubset(_collect_unique_values(profiler.get_all("objects/attribute_value"))))


class TestForbidParentRemoval:
    def test(self, example_env):
        mother_ship = example_env.create_mother_ship()
        example_env.client.remove_object(
            "mother_ship",
            str(mother_ship),
        )

        mother_ship = example_env.create_mother_ship()
        example_env.client.create_object(
            "interceptor",
            {"meta": {"mother_ship_id": int(mother_ship)}}
        )
        with pytest.raises(RemovalForbiddenError):
            example_env.client.remove_object(
                "mother_ship",
                str(mother_ship),
            )

        nexus_key = example_env.client.create_object("nexus", request_meta_response=True)
        mother_ship_key = example_env.client.create_object(
            "mother_ship",
            attributes={"meta": {"parent_key": nexus_key}},
            request_meta_response=True,
        )
        example_env.client.create_object(
            "interceptor",
            dict(meta=dict(parent_key=mother_ship_key))
        )
        with pytest.raises(RemovalForbiddenError):
            example_env.client.remove_object(
                "nexus",
                str(nexus_key),
            )
        example_env.client.remove_object(
            "nexus",
            str(nexus_key),
            allow_removal_with_non_empty_reference=True
        )
