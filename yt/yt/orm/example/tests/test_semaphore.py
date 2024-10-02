from yt.orm.tests.base_object_test import BaseObjectTest

from yt.common import YtResponseError

import pytest
import time


LARGE_LEASE_DURATION = 100_000


def _acquire(example_env, semaphore_id, leases_uuid_and_duration):
    example_env.client.update_object(
        "semaphore",
        semaphore_id,
        set_updates=[
            {
                "path": "/control/acquire",
                "value": {
                    "lease_uuid": lease_uuid,
                    "duration": lease_duration_ms,
                },
            } for lease_uuid, lease_duration_ms in leases_uuid_and_duration
        ],
    )


def _release(example_env, semaphore_id, leases_uuid):
    example_env.client.update_object(
        "semaphore",
        semaphore_id,
        set_updates=[
            {
                "path": "/control/release",
                "value": {
                    "lease_uuid": lease_uuid,
                },
            } for lease_uuid in leases_uuid
        ],
    )


def _get_fresh_leases(example_env, semaphore_id):
    return example_env.client.get_object("semaphore", semaphore_id, selectors=["/status/fresh_leases"])[
        0
    ]


class TestSemaphore(BaseObjectTest):
    @pytest.fixture
    def object_type(self):
        return "semaphore"

    @pytest.fixture
    def object_meta(self, example_env):
        return {"semaphore_set_id": example_env.client.create_object("semaphore_set")}

    @pytest.fixture
    def object_spec(self):
        return {"budget": 100}

    @pytest.fixture
    def set_updates(self, example_env):
        return [{"path": "/spec/budget", "value": 50}]

    def test_base(self, example_env):
        semaphore_set_id = example_env.client.create_object("semaphore_set")
        semaphore_id = example_env.client.create_object(
            "semaphore",
            attributes={
                "meta": {"semaphore_set_id": semaphore_set_id},
                "spec": {"budget": 100},
            },
        )

        lease_duration_ms = 5000
        example_env.client.update_object(
            "semaphore",
            semaphore_id,
            set_updates=[
                {
                    "path": "/control/acquire",
                    "value": {
                        "lease_uuid": "test_lease_uuid",
                        "duration": lease_duration_ms,
                    },
                },
            ],
        )

        leases = example_env.client.get_object("semaphore", semaphore_id, selectors=["/status/leases"])[
            0
        ]
        assert leases["test_lease_uuid"]["last_acquire_time"] > 0
        assert (
            leases["test_lease_uuid"]["expiration_time"]
            == leases["test_lease_uuid"]["last_acquire_time"] + lease_duration_ms
        )

        example_env.client.update_object(
            "semaphore",
            semaphore_id,
            set_updates=[
                {
                    "path": "/control/ping",
                    "value": {
                        "lease_uuid": "test_lease_uuid",
                        "duration": lease_duration_ms,
                    },
                },
            ],
        )

        time.sleep(lease_duration_ms / 1000.0)

        # Ping on expired lease should fail.
        with pytest.raises(YtResponseError):
            example_env.client.update_object(
                "semaphore",
                semaphore_id,
                set_updates=[
                    {
                        "path": "/control/ping",
                        "value": {
                            "lease_uuid": "test_lease_uuid",
                            "duration": lease_duration_ms,
                        },
                    },
                ],
            )

        # Release should never fail.
        example_env.client.update_object(
            "semaphore",
            semaphore_id,
            set_updates=[{"path": "/control/release", "value": {"lease_uuid": "test_lease_uuid"}}],
        )

    def test_fresh_leases(self, example_env):
        semaphore_set_id = example_env.client.create_object("semaphore_set")
        semaphore_id = example_env.client.create_object(
            "semaphore",
            attributes={"spec": {"budget": 100}, "meta": {"semaphore_set_id": semaphore_set_id}},
        )

        # No leases are taken after semaphore creation.
        fresh_leases = _get_fresh_leases(example_env, semaphore_id)
        assert fresh_leases == dict()

        lease_uuid = "test_lease_uuid"

        # Acquire => the lease must be present in fresh_leases.
        _acquire(example_env, semaphore_id, [(lease_uuid, LARGE_LEASE_DURATION)])
        fresh_leases = _get_fresh_leases(example_env, semaphore_id)
        assert list(fresh_leases.keys()) == [lease_uuid]

        lease = fresh_leases[lease_uuid]
        assert lease['budget'] == 1
        assert lease['expiration_time'] - lease['last_acquire_time'] == LARGE_LEASE_DURATION

        # Release => lease must no longer be present.
        _release(example_env, semaphore_id, [lease_uuid])
        fresh_leases = _get_fresh_leases(example_env, semaphore_id)
        assert list(fresh_leases.keys()) == []

    def test_fresh_leases_multi_acquire(self, example_env):
        semaphore_set_id = example_env.client.create_object("semaphore_set")
        assert semaphore_set_id
        budget = 100
        semaphore_id = example_env.client.create_object(
            "semaphore",
            attributes={"spec": {"budget": budget}, "meta": {"semaphore_set_id": semaphore_set_id}},
        )
        assert semaphore_set_id

        lease_uuids = []

        # Acquire all available leases => all acquired leases must be present in fresh_leases.
        while len(lease_uuids) < budget:
            lease_uuid = f"test_lease_uuid_{len(lease_uuids)}"
            lease_uuids.append(lease_uuid)
            _acquire(example_env, semaphore_id, [(lease_uuid, LARGE_LEASE_DURATION)])
        fresh_leases = _get_fresh_leases(example_env, semaphore_id)
        assert sorted(fresh_leases) == sorted(lease_uuids)

        # Release all leases => all leases are expired, fresh_leases is empty.
        _release(example_env, semaphore_id, lease_uuids)
        fresh_leases = _get_fresh_leases(example_env, semaphore_id)
        assert list(fresh_leases.keys()) == []

    def test_fresh_leases_with_expiration(self, example_env):
        semaphore_set_id = example_env.client.create_object("semaphore_set")
        assert semaphore_set_id
        semaphore_id = example_env.client.create_object(
            "semaphore",
            attributes={"spec": {"budget": 100}, "meta": {"semaphore_set_id": semaphore_set_id}},
        )
        assert semaphore_set_id

        short_lease_uuid, short_lease_duration_ms = "test_lease_uuid_short", 10_000
        long_lease_uuid, long_lease_duration_ms = "test_lease_uuid_long", 20_000

        # Acquire both leases, both must be fresh.
        _acquire(
            example_env,
            semaphore_id,
            [
                (short_lease_uuid, short_lease_duration_ms),
                (long_lease_uuid, long_lease_duration_ms),
            ]
        )
        fresh_leases = _get_fresh_leases(example_env, semaphore_id)
        assert sorted(fresh_leases) == sorted([short_lease_uuid, long_lease_uuid])

        time.sleep(short_lease_duration_ms / 1000)

        # Short lease is expired, only long is fresh.
        fresh_leases = _get_fresh_leases(example_env, semaphore_id)
        assert list(fresh_leases.keys()) == [long_lease_uuid]

        time.sleep((long_lease_duration_ms - short_lease_duration_ms) / 1000)

        # Both leases are expired.
        fresh_leases = _get_fresh_leases(example_env, semaphore_id)
        assert list(fresh_leases.keys()) == []
