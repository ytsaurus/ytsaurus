from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, create_medium, get, get_account_disk_space_limit,
    map_reduce, release_breakpoint, set, set_account_disk_space_limit,
    wait, wait_breakpoint, with_breakpoint, write_table,
)

from time import sleep

import builtins
import pytest

##################################################################


def get_operation_job_types(op):
    op_path = op.get_path()
    tasks = get(f"{op_path}/@progress/tasks")
    job_types = [task["job_type"] for task in tasks]
    return builtins.set(job_types)


def get_operation_tasks(op):
    op_path = op.get_path()
    data_flow = get(f"{op_path}/@progress/data_flow")
    tasks = []
    for direction in data_flow:
        if direction["source_name"] != "input":
            tasks.append(direction["source_name"])
        if direction["target_name"] != "output":
            tasks.append(direction["target_name"])
    return builtins.set(tasks)


class TestIntermediateMediumSwitch(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    STORE_LOCATION_COUNT = 2

    FAST_MEDIUM = "ssd_blobs"
    SLOW_MEDIUM = "default"

    INTERMEDIATE_ACCOUNT_USAGE_UPDATE_PERIOD = 0.1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "intermediate_account_usage_update_period": INTERMEDIATE_ACCOUNT_USAGE_UPDATE_PERIOD,
            "fast_intermediate_medium": "ssd_blobs",
            "fast_intermediate_medium_limit": 1 << 30,
        }
    }

    @classmethod
    def setup_class(cls):
        super(TestIntermediateMediumSwitch, cls).setup_class()
        disk_space_limit = get_account_disk_space_limit("tmp", "default")
        set_account_disk_space_limit("tmp", disk_space_limit, TestIntermediateMediumSwitch.FAST_MEDIUM)

    @classmethod
    def modify_node_config(cls, config):
        assert len(config["data_node"]["store_locations"]) == 2

        config["data_node"]["store_locations"][0]["medium_name"] = cls.SLOW_MEDIUM
        config["data_node"]["store_locations"][1]["medium_name"] = cls.FAST_MEDIUM

    @classmethod
    def on_masters_started(cls):
        create_medium(cls.FAST_MEDIUM)

    @authors("galtsev")
    @pytest.mark.timeout(600)
    def test_intermediate_medium_switch(self):
        def set_limit(account, medium, limit):
            set(f"//sys/accounts/{account}/@resource_limits/disk_space_per_medium/{fast_medium}", limit)

        def get_usage(account, medium, usage_type):
            return get(f"//sys/accounts/{account}/@{usage_type}/disk_space_per_medium/{medium}")

        def wait_usage_update():
            """
            Wait for the update of the information on the fast medium usage.
            """
            sleep(10 * self.INTERMEDIATE_ACCOUNT_USAGE_UPDATE_PERIOD)

        account = "intermediate"
        fast_medium = self.FAST_MEDIUM
        slow_medium = self.SLOW_MEDIUM

        def get_intermediate_usage(medium):
            return (
                get_usage(account, medium, "resource_usage") -
                get_usage(account, medium, "committed_resource_usage")
            )

        set_limit(account, fast_medium, 1 << 30)

        in_table = "//tmp/in"
        out_table = "//tmp/out"
        create(
            "table",
            in_table,
            attributes={"schema": [
                {"name": "key", "type_v3": "int64"},
                {"name": "value", "type_v3": "string"},
            ]},
        )
        data = [{"key": i % 100, "value": "#" * 1000000} for i in range(1000, 1, -1)]
        for d in data:
            write_table(f"<append=%true>{in_table}", [d])

        create("table", out_table)

        fast_intermediate_medium_usage = get_intermediate_usage(fast_medium)

        for medium in [fast_medium, slow_medium]:
            assert get_intermediate_usage(medium) == 0

        op = map_reduce(
            wait_for_jobs=True,
            track=False,
            in_=in_table,
            out=out_table,
            reduce_by="key",
            sort_by="key",
            mapper_command=with_breakpoint('if [ "$YT_JOB_INDEX" != "0" ]; then BREAKPOINT; fi; cat'),
            reducer_command=with_breakpoint("cat; BREAKPOINT"),
            spec={
                "data_weight_per_sort_job": 1,
                "fast_intermediate_medium_limit": 1,
                "partition_count": 10,
                "partition_job_count": 10,
                "partition_job_io": {
                    "table_writer": {
                        "block_size": 1024,
                        "desired_chunk_size": 1,
                    }
                },
                "resource_limits": {"user_slots": 1},
            })

        job_id = wait_breakpoint(job_count=1)[0]
        wait_usage_update()
        fast_intermediate_medium_usage = get_intermediate_usage(fast_medium)
        release_breakpoint(job_id=job_id)

        jobs_used_fast_medium = 1 if fast_intermediate_medium_usage > 0 else 0
        jobs_used_slow_medium = 0

        while op.get_job_count("completed") != op.get_job_count("total"):
            job_id = wait_breakpoint(job_count=1)[0]
            if fast_intermediate_medium_usage == get_intermediate_usage(fast_medium):
                jobs_used_slow_medium += 1
            else:
                jobs_used_fast_medium += 1
            wait_usage_update()
            completed_job_count = op.get_job_count("completed")
            release_breakpoint(job_id=job_id)
            wait(lambda: op.get_state() != "running" or op.get_job_count("completed") > completed_job_count, timeout=600)

        assert jobs_used_slow_medium > 0 and jobs_used_fast_medium <= 2, f"usage of the medium '{fast_medium}' exceeded"

        for medium in [fast_medium, slow_medium]:
            assert get_intermediate_usage(medium) > 0, f"the medium '{medium}' was not used at all"

        assert get_operation_job_types(op) == builtins.set([
            "intermediate_sort", "partition_map", "sorted_reduce"])
        assert get_operation_tasks(op) == builtins.set([
            "intermediate_sort", "partition_map(0)", "sorted_reduce"])
