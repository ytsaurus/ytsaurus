from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, write_table, read_table, merge, events_on_fs)

import time
import pytest


##################################################################


class TestEventsOnFs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "testing_options": {
                "enable_events_on_fs": True,
            },
        },
    }

    @authors("coteeq")
    @pytest.mark.parametrize("release", ["job", "all"])
    def test_events_on_fs(self, release):
        events_on_fs().external_breakpoint("before_run")
        create("table", "//tmp/t_in")
        data = [{"key": i, "value": f"value{i}"} for i in range(10)]
        write_table("//tmp/t_in", data)

        create("table", "//tmp/t_out")

        # Start merge operation with events_on_fs configured.
        op = merge(
            track=False,
            mode="ordered",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "job_testing_options": {
                    "events_on_fs": {
                        "path": events_on_fs().get_path(),
                        "breakpoints": ["before_run"],
                    },
                },
                "force_transform": True,
            },
        )

        job, = events_on_fs().wait_breakpoint("before_run", job_count=1)

        time.sleep(5)
        assert op.get_state() == "running"
        assert set(op.get_running_jobs().keys()) == {job}

        match release:
            case "all":
                events_on_fs().release_breakpoint("before_run")
            case "job":
                events_on_fs().release_breakpoint("before_run", job_id=job)
            case _:
                assert False

        op.track()

        assert read_table("//tmp/t_out") == data
