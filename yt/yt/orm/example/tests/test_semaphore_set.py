from yt.orm.tests.base_object_test import BaseObjectTest

from yt.common import wait

import pytest
import time


LARGE_LEASE_DURATION = 100_000


def _acquire(example_env, semaphore_set_id, lease_uuid, lease_duration_ms):
    example_env.client.update_object(
        "semaphore_set",
        semaphore_set_id,
        set_updates=[
            {
                "path": "/control/acquire",
                "value": {
                    "lease_uuid": lease_uuid,
                    "duration": lease_duration_ms,
                },
            }
        ],
    )


def _release(example_env, semaphore_set_id, lease_uuid):
    example_env.client.update_object(
        "semaphore_set",
        semaphore_set_id,
        set_updates=[{"path": "/control/release", "value": {"lease_uuid": lease_uuid}}],
    )


def _get_fresh_leases(example_env, semaphore_set_id):
    fresh_leases = example_env.client.get_object(
        "semaphore_set", semaphore_set_id, selectors=["/status/fresh_leases_by_semaphore"]
    )[0]
    for semaphore_id in fresh_leases.keys():
        fresh_leases[semaphore_id] = fresh_leases[semaphore_id]["values"]
    return fresh_leases


class TestSemaphoreSet(BaseObjectTest):
    @pytest.fixture
    def object_type(self):
        return "semaphore_set"

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
            "semaphore_set",
            semaphore_set_id,
            set_updates=[
                {
                    "path": "/control/acquire",
                    "value": {
                        "lease_uuid": "test_lease_uuid",
                        "duration": lease_duration_ms,
                    },
                }
            ],
        )

        assert len(example_env.client.get_object("semaphore", semaphore_id, selectors=["/status/leases"])[0]) == 1

    def test_fresh_leases(self, example_env):
        semaphore_set_id = example_env.client.create_object("semaphore_set")
        semaphore_ids = []
        for _ in range(5):
            semaphore_id = example_env.client.create_object(
                "semaphore",
                attributes={
                    "spec": {"budget": 100},
                    "meta": {"semaphore_set_id": semaphore_set_id},
                },
            )
            semaphore_ids.append(semaphore_id)

        # Semaphore_set must contain information on all semaphores,
        # But no fresh leases must be present in a newly created semaphore_set.
        fresh_leases = _get_fresh_leases(example_env, semaphore_set_id)
        assert sorted(fresh_leases.keys()) == sorted(semaphore_ids)
        assert all(list(semaphore_leases.keys()) == [] for semaphore_leases in fresh_leases.values())

        lease_uuid = "test_lease_uuid"
        _acquire(example_env, semaphore_set_id, lease_uuid, LARGE_LEASE_DURATION)

        # Exactly 1 lease exists and it has correct uuid.
        fresh_leases = _get_fresh_leases(example_env, semaphore_set_id)
        assert sorted(fresh_leases.keys()) == sorted(semaphore_ids)
        assert sum(len(semaphore_leases) for semaphore_leases in fresh_leases.values()) == 1
        assert sum(lease_uuid in semaphore_leases for semaphore_leases in fresh_leases.values()) == 1

        _release(example_env, semaphore_set_id, lease_uuid)

        # Lease was released => fresh_leases must be empty.
        fresh_leases = _get_fresh_leases(example_env, semaphore_set_id)
        assert all(list(semaphore_leases.keys()) == [] for semaphore_leases in fresh_leases.values())

    def test_fresh_leases_multi_acquire(self, example_env):
        semaphore_set_id = example_env.client.create_object("semaphore_set")
        semaphore_ids = []
        semaphore_budget = 100
        semaphore_ids = example_env.client.create_objects(
            (
                ("semaphore", dict(meta=dict(semaphore_set_id=semaphore_set_id), spec=dict(budget=semaphore_budget)))
                for _ in range(5)
            ),
        )

        fresh_leases = _get_fresh_leases(example_env, semaphore_set_id)
        assert sorted(fresh_leases.keys()) == sorted(semaphore_ids)
        assert all(list(semaphore_leases.keys()) == [] for semaphore_leases in fresh_leases.values())

        lease_uuids = []

        # Split leases between semaphores.
        for i in range(semaphore_budget):
            lease_uuid = f"test_lease_uuid_{i}"
            lease_uuids.append(lease_uuid)
            _acquire(example_env, semaphore_set_id, lease_uuid, LARGE_LEASE_DURATION)

        # Number of fresh leases must equal to the number of acquired leases.
        fresh_leases = _get_fresh_leases(example_env, semaphore_set_id)
        assert sum(len(semaphore_leases) for semaphore_leases in fresh_leases.values()) == len(lease_uuids)

        for lease_uuid in lease_uuids:
            # Each lease_uuid must be owned by exactly 1 semaphore.
            assert sum(lease_uuid in semaphore_leases for semaphore_leases in fresh_leases.values()) == 1
            _release(example_env, semaphore_set_id, lease_uuid)

        # All leases were released => fresh leases must be empty.
        fresh_leases = _get_fresh_leases(example_env, semaphore_set_id)
        assert all(list(semaphore_leases.keys()) == [] for semaphore_leases in fresh_leases.values())

    def test_fresh_leases_with_expiration(self, example_env):
        semaphore_set_id = example_env.client.create_object("semaphore_set")
        semaphore_ids = []
        for _ in range(5):
            semaphore_id = example_env.client.create_object(
                "semaphore",
                attributes={
                    "spec": {"budget": 100},
                    "meta": {"semaphore_set_id": semaphore_set_id},
                },
            )
            semaphore_ids.append(semaphore_id)

        short_lease_uuid, short_lease_duration_ms, short_lease_count = (
            "short_test_lease_uuid",
            10_000,
            10,
        )
        long_lease_uuid, long_lease_duration_ms, long_lease_count = (
            "long_test_lease_uuid",
            20_000,
            10,
        )
        lease_values = [
            (short_lease_uuid, short_lease_duration_ms, short_lease_count),
            (long_lease_uuid, long_lease_duration_ms, long_lease_count),
        ]

        def acquire():
            for lease_uuid, duration_ms, count in lease_values:
                for i in range(count):
                    _acquire(example_env, semaphore_set_id, lease_uuid + str(i), duration_ms)

        def get_count():
            fresh_leases = _get_fresh_leases(example_env, semaphore_set_id)
            return sum(len(semaphore_leases) for semaphore_leases in fresh_leases.values())

        # No leases are expired yet.
        def assert_full():
            acquire()
            return get_count() == short_lease_count + long_lease_count

        # Short leases expired, only longer leases are still fresh.
        def assert_partial():
            acquire()
            time.sleep(short_lease_duration_ms / 1000)
            if get_count() != long_lease_count:
                return False
            for semaphore_leases in _get_fresh_leases(example_env, semaphore_set_id).values():
                for lease in semaphore_leases:
                    if not lease.startswith(long_lease_uuid):
                        return False
            return True

        # Only uuids of longer leases are present.
        def assert_empty():
            acquire()
            time.sleep(long_lease_duration_ms / 1000)
            return get_count() == 0

        wait(assert_full)

        wait(assert_partial)

        wait(assert_empty)
