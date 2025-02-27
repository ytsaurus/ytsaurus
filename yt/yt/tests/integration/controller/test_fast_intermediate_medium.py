from yt_fast_intermediate_medium_base import TestFastIntermediateMediumBase

from yt_commands import (
    authors, create, exists, get, ls, map_reduce, release_breakpoint,
    wait, wait_breakpoint, with_breakpoint, write_table,
)

from time import sleep

import builtins
import datetime
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


@pytest.mark.enabled_multidaemon
class TestFastIntermediateMedium(TestFastIntermediateMediumBase):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 9
    NUM_SCHEDULERS = 1

    INTERMEDIATE_ACCOUNT_USAGE_UPDATE_PERIOD = 0.1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "intermediate_account_usage_update_period": INTERMEDIATE_ACCOUNT_USAGE_UPDATE_PERIOD,
            "fast_intermediate_medium": "ssd_blobs",
            "fast_intermediate_medium_limit": TestFastIntermediateMediumBase.FAST_INTERMEDIATE_MEDIUM_LIMIT,
        }
    }

    @classmethod
    def setup_class(cls):
        super(TestFastIntermediateMedium, cls).setup_class()

    @authors("galtsev")
    @pytest.mark.timeout(600)
    def test_intermediate_medium_switch(self):
        def get_usage(account, medium, usage_type):
            return get(f"//sys/accounts/{account}/@{usage_type}/disk_space_per_medium/{medium}")

        def wait_usage_update():
            """
            Wait for the update of the information on the fast medium usage.
            """
            sleep(10 * self.INTERMEDIATE_ACCOUNT_USAGE_UPDATE_PERIOD)

        account = self.INTERMEDIATE_ACCOUNT
        fast_medium = self.FAST_MEDIUM
        slow_medium = self.SLOW_MEDIUM

        def get_intermediate_usage(medium):
            return (
                get_usage(account, medium, "resource_usage") -
                get_usage(account, medium, "committed_resource_usage")
            )

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
        data = [{"key": i % 10, "value": "#" * 1000000} for i in range(100, 1, -1)]
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
                "data_weight_per_reduce_job": 1,
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
            job_id = wait_breakpoint(job_count=1, timeout=datetime.timedelta(seconds=300))[0]
            current_fast_intermediate_medium_usage = get_intermediate_usage(fast_medium)
            if fast_intermediate_medium_usage == current_fast_intermediate_medium_usage:
                jobs_used_slow_medium += 1
            else:
                jobs_used_fast_medium += 1
            fast_intermediate_medium_usage = current_fast_intermediate_medium_usage
            wait_usage_update()
            completed_job_count = op.get_job_count("completed")
            release_breakpoint(job_id=job_id)
            wait(lambda: op.get_state() != "running" or op.get_job_count("completed") > completed_job_count, timeout=600)

        assert jobs_used_slow_medium > 0 and jobs_used_fast_medium <= 2, f"usage of the medium '{fast_medium}' exceeded"

        for medium in [fast_medium, slow_medium]:
            assert get_intermediate_usage(medium) > 0, f"the medium '{medium}' was not used at all"

    @authors("galtsev")
    @pytest.mark.parametrize("fast_intermediate_config", [
        (1, None, False),
        (4, None, False),
        (1, "isa_reed_solomon_6_3", False),
        (1, "isa_reed_solomon_6_3", True),
    ])
    def test_intermediate_writer_config(self, fast_intermediate_config):
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (24, 2) or self.Env.get_component_version("ytserver-job-proxy").abi <= (24, 2):
            pytest.skip()

        (upload_replication_factor, erasure_codec, enable_striped_erasure) = fast_intermediate_config

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        row_count = 3
        write_table("//tmp/t_in", [{"x": x, "y": 1} for x in range(row_count)])

        fast_intermediate_medium_table_writer_config = {
            "upload_replication_factor": upload_replication_factor,
        }
        if erasure_codec is not None:
            fast_intermediate_medium_table_writer_config["erasure_codec"] = erasure_codec
            fast_intermediate_medium_table_writer_config["enable_striped_erasure"] = enable_striped_erasure

        def find_intermediate_chunks():
            return [
                str(chunk)
                for chunk in ls("//sys/chunks", attributes=["requisition"])
                if chunk.attributes["requisition"][0]["account"] == self.INTERMEDIATE_ACCOUNT
            ]

        assert not find_intermediate_chunks()

        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reduce_by="x",
            sort_by="x",
            spec={
                "fast_intermediate_medium_limit": self.FAST_INTERMEDIATE_MEDIUM_LIMIT,
                "fast_intermediate_medium_table_writer_config": fast_intermediate_medium_table_writer_config,
                "data_size_per_map_job": 1,
                "map_job_io": {"table_writer": {"desired_chunk_size": 1}}
            },
            mapper_command="cat",
            reducer_command=with_breakpoint("BREAKPOINT; cat"),
            track=False,
        )

        wait(lambda: len(find_intermediate_chunks()) >= row_count)

        def get_chunk_erasure_codec(chunk):
            path = f"#{intermediate_chunk}/@erasure_codec"
            if exists(path):
                return get(path)
            else:
                return None

        for intermediate_chunk in find_intermediate_chunks():
            assert get_chunk_erasure_codec(intermediate_chunk) == erasure_codec
            assert get(f"#{intermediate_chunk}/@media/{self.FAST_MEDIUM}/replication_factor") == upload_replication_factor

        release_breakpoint()
        op.track()
