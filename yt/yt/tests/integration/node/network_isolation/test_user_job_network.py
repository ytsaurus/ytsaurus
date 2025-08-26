from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, wait_no_assert, events_on_fs,
    get, set, get_job, run_test_vanilla,
    create_network_project, extract_statistic_v2)

from yt_helpers import profiler_factory

from yt.common import get_fqdn

##################################################################


class TestUserJobNetwork(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                    "porto_executor": {
                        "enable_network_isolation": True,
                    }
                },
            }
        }
    }

    DELTA_LOCAL_YT_CONFIG = {
        "fqdn": get_fqdn(),
    }

    USE_PORTO = True

    @authors("ignat")
    def test_network_statistics(self):
        create_network_project("n")
        # Project id of '_YT_JOBS_TEST_NETS_'
        set("//sys/network_projects/n/@project_id", 0xf420)

        op = run_test_vanilla(
            " ; ".join(
                [
                    events_on_fs().breakpoint_cmd("command_started"),
                    "curl http://{}/".format(get_fqdn()),
                    events_on_fs().breakpoint_cmd("command_finished"),
                ]),
            task_patch={
                "network_project": "n",
                "monitoring": {
                    "enable": True,
                    "sensor_names": ["network/rx_bytes", "network/tx_bytes"],
                },
            },
        )

        job_id = events_on_fs().wait_breakpoint("command_started")[0]

        @wait_no_assert
        def check():
            statistics = get(op.get_path() + "/controller_orchid/progress/job_statistics_v2")
            for stat_key in ("tx_bytes", "tx_packets", "tx_drops", "rx_bytes", "rx_packets", "rx_drops"):
                stat_value = extract_statistic_v2(
                    statistics,
                    key="user_job.network." + stat_key,
                    job_state="running",
                    job_type="task",
                    summary_type="sum")
                assert stat_value is not None

        wait(lambda: "monitoring_descriptor" in get_job(op.id, job_id))

        job = get_job(op.id, job_id)
        node = job["address"]
        descriptor = job["monitoring_descriptor"]

        profiler = profiler_factory().at_node(node)
        counter = profiler.counter("user_job/network/rx_bytes", tags={"job_descriptor": descriptor})

        events_on_fs().release_breakpoint("command_started")

        wait(lambda: counter.get() is not None)
        # TODO(ignat): investigate flaps of network accounting.
        # wait(lambda: counter.get_delta() > 0)

        events_on_fs().release_breakpoint("command_finished")

        op.track()
