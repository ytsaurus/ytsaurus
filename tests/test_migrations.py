from .conftest import (
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
)

from yp.local import (
    ACTUAL_DB_VERSION,
    DbManager,
)

from yp.common import (
    YpTimestampOutOfRangeError,
    YtResponseError,
    wait,
)

import pytest


@pytest.mark.usefixtures("yp_env")
class TestMigrations(object):
    def _emulate_migration(self, yp_env, table_names):
        def mapper(row):
            yield row
        db_manager = DbManager(yp_env.yt_client, "//yp", version=ACTUAL_DB_VERSION)
        for table_name in table_names:
            db_manager.run_map(table_name, mapper)
        db_manager.mount_unmounted_tables()
        db_manager.finalize()

    def test_finalization_timestamp(self, yp_env):
        yt_client = yp_env.yt_client

        assert yt_client.get("//yp/db/@finalization_timestamp") > 0

    def test_read_with_old_timestamp_after_migration_is_forbidden(self, yp_env):
        yt_client = yp_env.yt_client
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id)

        def check(timestamp=None):
            return [[pod_id]] == yp_client.select_objects(
                "pod",
                filter="[/meta/pod_set_id] = \"{}\"".format(pod_set_id),
                selectors=["/meta/id"],
                timestamp=timestamp,
            )

        old_timestamp = yp_client.generate_timestamp()

        assert check(old_timestamp)

        self._emulate_migration(yp_env, ("pods",))

        # Force instance to reconnect.
        def get_transaction_id():
            instance = yt_client.list("//yp/master/instances")[0]
            locks = yt_client.get("//yp/master/instances/{}/@locks".format(instance))
            if not locks:
                return None
            return locks[0]["transaction_id"]

        old_transaction_id = get_transaction_id()
        assert old_transaction_id
        yt_client.abort_transaction(old_transaction_id)
        wait(lambda: not (get_transaction_id() in (None, old_transaction_id)))

        # Wait for reconnection and check read with new timestamp.
        wait(check)

        # Read with old timestamp and ensure that master will return an error.
        with pytest.raises(YpTimestampOutOfRangeError):
            check(old_timestamp)

    def test_migration_preserves_sharding_attributes(self, yp_env):
        yt_client = yp_env.yt_client
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        pod_id = yp_client.create_object("pod", attributes=dict(meta=dict(pod_set_id=pod_set_id)))

        def wait_for_liveness():
            def check():
                try:
                    yp_client.get_object("pod_set", pod_set_id, selectors=["/meta/id"])
                    yp_client.get_object("pod", pod_id, selectors=["/meta/id"])
                    return True
                except YtResponseError:
                    return False
            wait(check)

        wait_for_liveness()

        pivot_keys = [
            [],
            ["a", "b"],
        ]

        desired_tablet_count = 2

        yt_client.unmount_table("//yp/db/pods", sync=True)
        yt_client.reshard_table("//yp/db/pods", pivot_keys, sync=True)
        yt_client.mount_table("//yp/db/pods", sync=True)

        yt_client.unmount_table("//yp/db/pod_sets", sync=True)
        yt_client.set_attribute("//yp/db/pod_sets", "desired_tablet_count", desired_tablet_count)
        yt_client.mount_table("//yp/db/pod_sets", sync=True)

        wait_for_liveness()

        self._emulate_migration(yp_env, ("pods", "pod_sets"))

        wait_for_liveness()

        assert yt_client.get("//yp/db/pods/@pivot_keys") == pivot_keys
        assert yt_client.get("//yp/db/pod_sets/@desired_tablet_count") == desired_tablet_count
