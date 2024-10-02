from yt.orm.tests.helpers import assert_over_time

from yt.orm.library.common import NotWatchableError

from datetime import timedelta
import pytest


class TestExecutor:
    EXAMPLE_MASTER_CONFIG = dict(
        watch_manager=dict(
            state_per_type_per_log=dict(
                executor=dict(
                    khalai_rank_watch_log="query_store", watch_log="none", selector_watch_log="none"
                )
            ),
            query_selector_enabled_per_type=dict(executor=True),
            query_filter_enabled_per_type=dict(executor=True),
            labels_store_enabled_per_type=dict(executor=True),
        ),
    )

    def test_watch_log(self, example_env):
        client = example_env.client

        start_timestamp = client.generate_timestamp()
        executor_id = client.create_object("executor", request_meta_response=True)
        timestamp = client.generate_timestamp()

        with pytest.raises(NotWatchableError):
            client.watch_objects(
                "executor",
                start_timestamp=start_timestamp,
                timestamp=timestamp,
                time_limit=timedelta(seconds=30),
            )

        client.update_object(
            "executor", executor_id, set_updates=[{"path": "/labels/tribe", "value": "Khalai"}]
        )

        start_timestamp = client.generate_timestamp()

        client.update_object(
            "executor",
            executor_id,
            set_updates=[{"path": "/spec/rank", "value": 3}],
        )

        timestamp = client.generate_timestamp()

        with pytest.raises(NotWatchableError):
            client.watch_objects(
                "executor",
                start_timestamp=start_timestamp,
                timestamp=timestamp,
                request_meta_response=True,
                time_limit=timedelta(seconds=30),
                selectors=["/spec/rank"],
            )

        with pytest.raises(NotWatchableError):
            client.watch_objects(
                "executor",
                start_timestamp=start_timestamp,
                timestamp=timestamp,
                request_meta_response=True,
                time_limit=timedelta(seconds=30),
                filter="[/labels/tribe] = 'Khalai'",
            )

        def check():
            result = client.watch_objects(
                "executor",
                start_timestamp=start_timestamp,
                timestamp=timestamp,
                request_meta_response=True,
                time_limit=timedelta(seconds=30),
                filter="[/labels/tribe] = 'Khalai'",
                selectors=["/spec/rank"],
                watch_log="khalai_rank_watch_log",
            )
            return len(result) == 1

        assert check()

        example_env.set_cypress_config_patch(
            dict(
                watch_manager=dict(
                    state_per_type_per_log=dict(
                        executor=dict(
                            khalai_rank_watch_log="query_store",
                            selector_watch_log="query_store",
                        )
                    )
                )
            ),
        )

        assert_over_time(check)
