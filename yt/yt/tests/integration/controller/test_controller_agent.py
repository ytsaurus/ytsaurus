from yt_env_setup import (
    YTEnvSetup,
    is_asan_build,
    Restarter,
    CONTROLLER_AGENTS_SERVICE,
)

from yt_commands import (
    authors, create_test_tables, print_debug, release_breakpoint, remove, wait, wait_breakpoint, with_breakpoint, create, ls, get,
    set, exists, update_op_parameters,
    write_table, map, reduce, map_reduce, merge, erase, vanilla, run_sleeping_vanilla, run_test_vanilla, get_operation, raises_yt_error)

from yt_helpers import profiler_factory

from yt.common import YtError, YtResponseError

import yt_error_codes

import yt.yson as yson

import pytest
from flaky import flaky

import time
from collections import defaultdict

##################################################################


class TestControllerAgentOrchid(YTEnvSetup):
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
            "snapshot_period": 3000,
        }
    }

    @authors("levysotsky")
    def test_controller_agent_orchid(self):
        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1

        controller_agent_orchid = "//sys/controller_agents/instances/{}/orchid/controller_agent".format(
            controller_agents[0]
        )

        wait(lambda: exists(controller_agent_orchid + "/incarnation_id"))

    @authors("akozhikhov")
    def test_list_operations_with_snapshots(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"foo": 0}])
        create("table", "//tmp/t2")

        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1
        orchid_path = "//sys/controller_agents/instances/{}/orchid/controller_agent/snapshotted_operation_ids".format(
            controller_agents[0]
        )

        assert list(get(orchid_path)) == []

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"data_size_per_job": 1},
        )

        assert list(get(orchid_path)) == []

        wait_breakpoint()
        op.wait_for_fresh_snapshot()

        assert list(get(orchid_path)) == [str(op.id)]


class TestControllerAgentConfig(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    @authors("ignat")
    def test_basic(self):
        assert get("//sys/controller_agents/config/@type") == "document"


class TestControllerAgentRegistration(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    @authors("pogorelov")
    def test_node_lock(self):
        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1

        node_path = "//sys/controller_agents/instances/{}".format(
            controller_agents[0]
        )

        with pytest.raises(YtResponseError):
            remove(node_path)


class TestControllerMemoryUsage(YTEnvSetup):
    NUM_SCHEDULERS = 1

    DELTA_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 5,
        }
    }

    @authors("ignat", "gepardo", "ni-stoiko")
    @pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
    def test_controller_memory_usage(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("<append=%true>//tmp/t_in", [{"a": 0}])

        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1

        controller_agent_orchid = "//sys/controller_agents/instances/{}/orchid/controller_agent".format(
            controller_agents[0]
        )

        def check(operation, usage_lower_bound, usage_upper_bound):
            state = operation.get_state()
            if state != "running":
                return False

            statistics = None

            for stat in get(controller_agent_orchid + "/tagged_memory_statistics"):
                if stat["operation_id"] == operation.id:
                    statistics = stat
                    break

            assert statistics

            if statistics["usage"] < usage_lower_bound:
                return False

            if statistics["usage"] > usage_upper_bound:
                return False

            return True

        for entry in get(controller_agent_orchid + "/tagged_memory_statistics", verbose=False):
            assert entry["operation_id"] == yson.YsonEntity()
            assert False, "Must not exist alive operations"

        # Used to pre-allocate internal structures that are laziliy initialized on first access.
        op_small_preallocation = map(
            track=False,
            in_="//tmp/t_in",
            out="<sorted_by=[a]>//tmp/t_out",
            command="sleep 3600",
        )
        wait(lambda: op_small_preallocation.get_job_count("running") > 0)
        op_small_preallocation.abort()

        op_small = map(
            track=False,
            in_="//tmp/t_in",
            out="<sorted_by=[a]>//tmp/t_out",
            command="sleep 3600",
            spec={
                "testing": {
                    "allocation_size": 20 * 1024 ** 2,
                    "allocation_release_delay": 60000,
                }
            },
        )

        small_usage_path = op_small.get_path() + "/controller_orchid/memory_usage"
        wait(lambda: exists(small_usage_path))
        wait(lambda: get(small_usage_path) > 0)
        wait(lambda: check(op_small, 0, 40 * 1024 ** 2))

        op_small.abort()

        op_large = map(
            track=False,
            in_="//tmp/t_in",
            out="<sorted_by=[a]>//tmp/t_out",
            command="sleep 3600",
            spec={
                "testing": {
                    "allocation_size": 300 * 1024 ** 2,
                    "allocation_release_delay": 60000,
                }
            },
        )

        large_usage_path = op_large.get_path() + "/controller_orchid/memory_usage"
        wait(lambda: exists(large_usage_path), sleep_backoff=0.01, timeout=5)
        wait(lambda: get(large_usage_path) > 10 * 1024 ** 2, sleep_backoff=0.01, timeout=30)

        peak_memory_usage = None

        def check_operation_attributes(op, usage_lower_bound):
            nonlocal peak_memory_usage
            info = get_operation(op.id, attributes=["memory_usage", "progress"], include_runtime=True)
            current_peak_memory_usage = info["progress"].get("peak_memory_usage")
            if current_peak_memory_usage is None:
                return False

            # Check that the attribute is monotonic.
            assert peak_memory_usage is None or current_peak_memory_usage >= peak_memory_usage
            peak_memory_usage = current_peak_memory_usage

            return info["memory_usage"] > usage_lower_bound and current_peak_memory_usage > usage_lower_bound

        wait(lambda: check_operation_attributes(op_large, 50 * 1024 ** 2), sleep_backoff=0.01, timeout=30)
        wait(lambda: check(op_large, 150 * 1024 ** 2, 500 * 1024 ** 2), sleep_backoff=0.01, timeout=10)

        op_large.abort()

        time.sleep(5)

        for i, entry in enumerate(get(controller_agent_orchid + "/tagged_memory_statistics", verbose=False)):
            if i <= 2:
                assert entry["operation_id"] in (op_small_preallocation.id, op_small.id, op_large.id)
            else:
                assert entry["operation_id"] == yson.YsonEntity()
            assert False, "Must not exist alive operations"


class TestControllerAgentMemoryPickStrategy(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 2

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "controller_agent_tracker": {
                "agent_pick_strategy": "memory_usage_balanced",
                "min_agent_available_memory": 0,
                "min_agent_available_memory_fraction": 0.0,
            }
        }
    }
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "memory_watchdog": {
                "memory_usage_check_period": 10,
                "total_controller_memory_limit": 800 * 1024 ** 2,
            },
        }
    }

    DELTA_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 10,
        }
    }

    DELTA_NODE_CONFIG = {"job_resource_manager": {"resource_limits": {"user_slots": 100, "cpu": 100}}}

    @authors("ignat", "ni-stoiko")
    # COMPAT(ni-stoiko): Enable after fix tcmalloc.
    @pytest.mark.skip(reason="The tcmalloc's patch 'user_data.patch' does NOT process user_data in StackTrace's hash")
    @pytest.mark.timeout(60)
    @pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
    def test_strategy(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        write_table("<append=%true>//tmp/t_in", [{"a": 0}])

        ops = []
        for i in range(40):
            out = "//tmp/t_out" + str(i)
            create("table", out, attributes={"replication_factor": 1})
            ops.append(map(
                command="sleep 1000",
                in_="//tmp/t_in",
                out=out,
                spec={
                    "testing": {
                        "allocation_size": 20 * 1024 ** 2,
                        "allocation_release_delay": 1000 ** 2,
                    }
                },
                track=False,
            ))

        def get_memory_usage(op):
            try:
                return get(op.get_path() + "/controller_orchid/memory_usage", verbose=False)
            except Exception:
                return 0

        address_to_operations = defaultdict(list)
        operation_to_address = {}
        operation_to_memory_usage = {}
        for op in ops:
            wait(lambda: op.get_state() == "running", sleep_backoff=0.01, timeout=10)
            wait(lambda: get_memory_usage(op) > 5 * 1024 ** 2, sleep_backoff=0.01, timeout=20)
            memory_usage = get_memory_usage(op)

            address = get(op.get_path() + "/@controller_agent_address")
            address_to_operations[address].append(op.id)
            operation_to_address[op.id] = address
            operation_to_memory_usage[op.id] = memory_usage
            print_debug(f"Operation id: {op.id}, controller agent address: {address}, memory usage: {memory_usage}")

        address_to_memory_usage = {
            address: sum(operation_to_memory_usage[op_id] for op_id in op_ids)
            for address, op_ids in address_to_operations.items()
        }

        balance_ratio = min(address_to_memory_usage.values()) / sum(address_to_memory_usage.values())

        print_debug(f"Balance ratio: {balance_ratio}")
        for op in ops:
            print_debug(
                op.id,
                operation_to_address[op.id],
                operation_to_memory_usage[op.id],
            )

        assert 0.3 <= balance_ratio <= 0.5


##################################################################


class TestSchedulerControllerThrottling(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {"scheduler": {"schedule_allocation_time_limit": 100, "operations_update_period": 10}}

    @authors("ignat")
    def test_time_based_throttling(self):
        create("table", "//tmp/input")

        testing_options = {"schedule_job_delay": {"duration": 200}}

        data = [{"foo": i} for i in range(5)]
        write_table("//tmp/input", data)

        create("table", "//tmp/output")
        op = map(
            track=False,
            in_="//tmp/input",
            out="//tmp/output",
            command="cat",
            spec={"testing": testing_options},
        )

        wait(lambda: get(op.get_path() + "/@progress/jobs", default=None) is not None)

        def get_progress_jobs():
            return get(op.get_path() + "/@progress/jobs", default=None)

        wait(lambda: get_progress_jobs()["aborted"]["non_scheduled"]["scheduling_timeout"] > 0)

        current_value = get_progress_jobs()["aborted"]["non_scheduled"]["scheduling_timeout"]

        wait(lambda: get_progress_jobs()["aborted"]["non_scheduled"]["scheduling_timeout"] > current_value)

        progress_jobs = get_progress_jobs()
        assert progress_jobs["completed"]["total"] == 0


##################################################################


class TestCustomControllerQueues(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "schedule_job_controller_queue": "schedule_job",
            "build_job_spec_controller_queue": "build_job_spec",
            "job_events_controller_queue": "job_events",
        }
    }

    @authors("ignat")
    def test_run_map(self):
        data = [{"foo": i} for i in range(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        map(
            command="sleep 10",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1, "locality_timeout": 0},
        )

    @authors("eshcherbin")
    def test_run_map_reduce(self):
        data = [{"foo": i} for i in range(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        map_reduce(
            mapper_command="cat",
            reducer_command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            sort_by=["foo"],
        )

    @authors("eshcherbin")
    def test_run_merge_erase(self):
        data = [{"foo": i} for i in range(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        merge(in_="//tmp/in", out="//tmp/out", spec={"force_transform": True})
        erase("//tmp/in")

    @authors("eshcherbin")
    def test_run_reduce(self):
        data = [{"foo": i} for i in range(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data, sorted_by=["foo"])

        reduce(command="sleep 1; cat", in_="//tmp/in", out="//tmp/out", reduce_by=["foo"])


##################################################################


class TestGetJobSpecFailed(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("ignat")
    def test_get_job_spec_failed(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in range(2)])

        op = map(
            command="sleep 100",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "testing": {"fail_get_job_spec": True},
            },
            track=False,
        )

        def check():
            jobs = get(op.get_path() + "/controller_orchid/progress/jobs", default=None)
            if jobs is None:
                return False
            return jobs["aborted"]["non_scheduled"]["get_spec_failed"] > 0

        wait(check)

    # NB: YT-15149
    @authors("eshcherbin")
    def test_get_job_spec_failed_with_fail_on_job_restart(self):
        op = run_sleeping_vanilla(
            spec={
                "testing": {"fail_get_job_spec": True},
                "fail_on_job_restart": True,
            },
        )

        def check():
            assert op.get_state() != "failed"
            return op.get_job_count("aborted") >= 5

        wait(check)


##################################################################


class TestControllerAgentTags(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 3

    @classmethod
    def modify_controller_agent_config(cls, config, cluster_index):
        if not hasattr(cls, "controller_agent_counter"):
            cls.controller_agent_counter = 0
        controller_agent_tag = None
        if cls.controller_agent_counter == 0:
            controller_agent_tag = "foo"
        elif cls.controller_agent_counter == 1:
            controller_agent_tag = "bar"
        cls.controller_agent_counter += 1
        if controller_agent_tag is not None:
            config["controller_agent"]["tags"] = [controller_agent_tag]
        config["controller_agent"]["controller_static_orchid_update_period"] = 100

    def _wait_for_tags_loaded(self):
        for agent in get("//sys/controller_agents/instances").keys():
            tags_path = "//sys/controller_agents/instances/{}/@tags".format(agent)
            wait(lambda: len(get(tags_path)) > 0)

    def _get_controller_tags(self):
        result = {}
        for agent in get("//sys/controller_agents/instances").keys():
            agent_tags = get("//sys/controller_agents/instances/{}/@tags".format(agent))
            assert len(agent_tags) == 1
            agent_tag = agent_tags[0]
            result[agent_tag] = agent
        return result

    def _get_controller_agent(self, op):
        attr_path = op.get_path() + "/@controller_agent_address"
        return get(attr_path, default=None)

    def _run_with_tag(self, tag):
        if tag is None:
            spec = {}
        else:
            spec = {"controller_agent_tag": tag}
        return run_test_vanilla("sleep 1000", job_count=1, spec=spec, track=False)

    def _reset_tags(self):
        for agent in get("//sys/controller_agents/instances").keys():
            if exists("//sys/controller_agents/instances/{}/@tags_override".format(agent)):
                remove("//sys/controller_agents/instances/{}/@tags_override".format(agent))
            remove("//sys/controller_agents/instances/{}/@tags".format(agent))

        # NB. We need to restart controller agents to apply the tags
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

    def _check_assignment(self, tag, agent):
        op = self._run_with_tag(tag)
        wait(lambda: self._get_controller_agent(op) == agent)

    def _check_no_agent(self, tag):
        op = self._run_with_tag(tag)
        wait(lambda: op.get_state() == "waiting_for_agent")
        time.sleep(1)
        assert self._get_controller_agent(op) is None

    @authors("gritukan")
    def test_controller_agent_tags(self):
        self._reset_tags()
        self._wait_for_tags_loaded()
        tags = self._get_controller_tags()
        foo_agent = tags["foo"]
        bar_agent = tags["bar"]
        default_agent = tags["default"]

        self._check_assignment("foo", foo_agent)
        self._check_assignment("bar", bar_agent)
        self._check_assignment(None, default_agent)
        self._check_no_agent("baz")

        # foo -> foo
        # bar -> baz
        # default -> boo, booo
        foo_agent, baz_agent, boo_agent = foo_agent, bar_agent, default_agent
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            set("//sys/controller_agents/instances/{}/@tags_override".format(baz_agent), ["baz"])
            set(
                "//sys/controller_agents/instances/{}/@tags_override".format(default_agent),
                ["boo", "booo"],
            )

        self._wait_for_tags_loaded()
        assert get("//sys/controller_agents/instances/{}/@tags".format(foo_agent)) == ["foo"]
        assert get("//sys/controller_agents/instances/{}/@tags".format(baz_agent)) == ["baz"]
        assert get("//sys/controller_agents/instances/{}/@tags".format(boo_agent)) == [
            "boo",
            "booo",
        ]

        self._check_assignment("foo", foo_agent)
        self._check_assignment("baz", baz_agent)
        self._check_assignment("boo", boo_agent)
        self._check_assignment("booo", boo_agent)
        self._check_no_agent(None)
        self._check_no_agent("bar")

    @authors("gepardo")
    def test_controller_agent_tags_reassign(self):
        self._reset_tags()
        self._wait_for_tags_loaded()
        tags = self._get_controller_tags()
        foo_agent = tags["foo"]
        bar_agent = tags["bar"]

        op = self._run_with_tag("foo")
        wait(lambda: self._get_controller_agent(op) == foo_agent)
        wait(lambda: op.get_state() == "running")
        update_op_parameters(op.id, parameters={"controller_agent_tag": "bar"})
        time.sleep(2.0)
        assert self._get_controller_agent(op) == foo_agent
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass
        wait(lambda: self._get_controller_agent(op) == bar_agent)


##################################################################


class TestOperationControllerResourcesCheck(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "available_exec_nodes_check_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {"job_resource_manager": {"resource_limits": {"cpu": 2.0}}}

    @authors("omgronny")
    def test_insufficient_resources_node_attributes(self):
        op = vanilla(
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 10,
                        "command": "sleep 1000",
                        "cpu_limit": 5.0,
                        "memory_limit": 1024,
                    },
                    "task_b": {
                        "job_count": 10,
                        "command": "sleep 1000",
                        "cpu_limit": 2.0,
                        "memory_limit": 1024 ** 4,
                    },
                }
            },
            track=False,
        )

        wait(lambda: op.get_state() == "failed")

        insufficient_resources_result = get(op.get_path() + "/@result")["error"]["attributes"]["matching_but_insufficient_resources_node_count_per_task"]

        assert insufficient_resources_result["task_a"]["cpu"] == 3
        assert insufficient_resources_result["task_b"]["cpu"] == 0

        assert insufficient_resources_result["task_a"]["memory"] == 0
        assert insufficient_resources_result["task_b"]["memory"] == 3

        assert insufficient_resources_result["task_a"]["network"] == 0
        assert insufficient_resources_result["task_b"]["network"] == 0

        assert insufficient_resources_result["task_a"]["disk_space"] == 0
        assert insufficient_resources_result["task_b"]["disk_space"] == 0

        assert insufficient_resources_result["task_a"]["user_slots"] == 0
        assert insufficient_resources_result["task_b"]["user_slots"] == 0


##################################################################


class TestOperationControllerLimit(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "memory_watchdog": {
                "memory_usage_check_period": 10,
                "operation_controller_memory_limit": 50 * 1024 ** 2,
            },
        }
    }

    DELTA_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 20,
        }
    }

    @authors("gritukan")
    @pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
    def test_operation_controller_memory_limit_exceeded(self):
        op = run_test_vanilla("sleep 60", job_count=1, spec={
            "testing": {
                "allocation_size": 200 * 1024 ** 2,
                "allocation_release_delay": 1000 ** 2,
            },
        })
        with raises_yt_error(yt_error_codes.ControllerMemoryLimitExceeded):
            op.track()


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
class TestMemoryOverconsumptionThreshold(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "memory_watchdog": {
                "memory_usage_check_period": 10,
                "operation_controller_memory_overconsumption_threshold": 50 * 1024 ** 2,
            },
        }
    }

    DELTA_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 10,
        }
    }

    @authors("alexkolodezny")
    def test_memory_overconsumption_threshold(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1, spec={
            "testing": {
                "allocation_size": 100 * 1024 ** 2,
                "allocation_release_delay": 1000 ** 2,
            },
        })

        wait(lambda: "memory_overconsumption" in op.get_alerts())
        wait_breakpoint()


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
class TestTotalControllerMemoryLimit(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "memory_watchdog": {
                "memory_usage_check_period": 10,
                "operation_controller_memory_overconsumption_threshold": 10 * 1024 ** 2,
                "total_controller_memory_limit": 50 * 1024 ** 2,
            },
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "controller_agent_tracker": {
                "min_agent_available_memory": 0,
                "min_agent_available_memory_fraction": 0.0
            },
        }
    }

    DELTA_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 10,
        }
    }

    @authors("alexkolodezny")
    def test_total_controller_memory_limit(self):
        op = run_test_vanilla(
            "sleep 30",
            job_count=1,
            track=False,
            spec={
                "testing": {
                    "allocation_size": 100 * 1024 ** 2,
                    "allocation_release_delay": 60000,
                },
            })

        time.sleep(1)

        with raises_yt_error(yt_error_codes.ControllerMemoryLimitExceeded):
            op.track()


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
class TestTotalControllerMemoryExceedLimit(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "memory_watchdog": {
                "memory_usage_check_period": 10,
                "total_controller_memory_limit": 100 * 1024 ** 2,
            },
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "controller_agent_tracker": {
                "min_agent_available_memory": 0,
                "min_agent_available_memory_fraction": 0.0
            },
        }
    }

    DELTA_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 10,
        }
    }

    @authors("ni-stoiko")
    def test_total_controller_memory_limit(self):
        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1
        agent_alerts_path = f"//sys/controller_agents/instances/{controller_agents[0]}/@alerts"

        run_test_vanilla(
            "sleep 30",
            job_count=1,
            track=False,
            spec={
                "testing": {
                    "allocation_size": 400 * 1024 ** 2,
                    "allocation_release_delay": 30000,
                },
            })

        wait(lambda: len(get(agent_alerts_path)) > 0)
        assert "Total controller memory usage of running operations exceeds limit" in str(get(agent_alerts_path))


##################################################################


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
class TestControllerAgentMemoryAlert(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "memory_watchdog": {
                "memory_usage_check_period": 10,
                "total_controller_memory_limit": 50 * 1024 ** 2,
                "operation_controller_memory_overconsumption_threshold": 100 * 1024 ** 2,
            },
        },
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "controller_agent_tracker": {
                "min_agent_available_memory": 0,
                "min_agent_available_memory_fraction": 0.0
            },
        }
    }

    DELTA_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 10,
        }
    }

    @authors("alexkolodezny", "ni-stoiko")
    # COMPAT(ni-stoiko): Enable after fix tcmalloc.
    @pytest.mark.skip(reason="The tcmalloc's patch 'user_data.patch' does NOT process user_data in StackTrace's hash")
    @flaky(max_runs=3)
    def test_controller_agent_memory_alert(self):
        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1
        agent_alerts_path = "//sys/controller_agents/instances/{}/@alerts".format(
            controller_agents[0]
        )

        def get_agent_alerts():
            return get(agent_alerts_path)
        op1 = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            spec={
                "testing": {
                    "allocation_size": 50 * 1024 ** 2,
                    "allocation_release_delay": 30000,
                },
            },
            track=False,
        )
        op2 = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            spec={
                "testing": {
                    "allocation_size": 50 * 1024 ** 2,
                    "allocation_release_delay": 30000,
                },
            },
            track=False,
        )
        op3 = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            spec={
                "testing": {
                    "allocation_size": 400 * 1024 ** 2,
                    "allocation_release_delay": 30000,
                },
            },
            track=False,
        )

        wait(lambda: len(get_agent_alerts()) > 0)

        assert "Total controller memory usage of running operations exceeds limit" in str(get_agent_alerts()[0])

        release_breakpoint()
        op1.track()
        op2.track()
        with raises_yt_error(yt_error_codes.ControllerMemoryLimitExceeded):
            op3.track()


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
class TestMemoryWatchdog(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 4
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "memory_watchdog": {
                "memory_usage_check_period": 20,
                "total_controller_memory_limit": 100 * 1024 * 1024,
                "operation_controller_memory_overconsumption_threshold": 30 * 1024 * 1024,
            },
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "controller_agent_tracker": {
                "min_agent_available_memory": 0,
                "min_agent_available_memory_fraction": 0.0
            },
        }
    }

    DELTA_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 20,
        }
    }

    @authors("alexkolodezny")
    @pytest.mark.skip(reason="YT-20965")
    @flaky(max_runs=3)
    @pytest.mark.timeout(30)
    def test_memory_watchdog(self):
        big_ops = []
        for _ in range(2):
            big_ops.append(run_test_vanilla(
                with_breakpoint("BREAKPOINT; sleep 5"),
                spec={
                    "testing": {
                        "allocation_size": 50 * 1024 * 1024,
                        "allocation_release_delay": 3000,
                    },
                },
                track=False,
            ))

        wait_breakpoint()
        release_breakpoint()
        small_ops = []
        for _ in range(2):
            small_ops.append(run_test_vanilla(
                "sleep 1",
                spec={
                    "testing": {
                        "allocation_size": 10 * 1024 * 1024,
                        "allocation_release_delay": 1000,
                    },
                },
                track=False,
            ))

        for op in small_ops:
            op.track()

        failed = 0
        for op in big_ops:
            try:
                op.track()
            except YtError as err:
                assert err.contains_code(yt_error_codes.ControllerMemoryLimitExceeded)
                failed += 1
        assert failed == 1

    @authors("arkady-e1ppa")
    def test_operation_controller_profiling(self):
        controller_agent = ls("//sys/controller_agents/instances")[0]
        profiler = profiler_factory().at_controller_agent(controller_agent)

        bucket_names = [
            "Default",
            "GetJobSpec",
            "JobEvents",
        ]

        counter_names = [
            "enqueued",
            "dequeued",
        ]

        counters = {
            bucket : {
                counter : profiler.counter(name=f"fair_share_invoker_pool/{counter}", tags={"thread": "OperationController", "bucket": bucket})
                for counter in counter_names
            }
            for bucket in bucket_names
        }

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1)

        wait_breakpoint()

        wait(lambda: all(counter["enqueued"].get_delta() > 0 and counter["dequeued"].get_delta() > 0 for _, counter in counters.items()))

        release_breakpoint()

        op.track()


class TestLivePreview(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_SECONDARY_MASTER_CELLS = 2

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "map_operation_options": {},
            "map_reduce_operation_options": {},
            "snapshot_period": 500,
        }
    }

    @authors("galtsev")
    @pytest.mark.parametrize("clean_start", [False, True])
    def test_do_not_crash_on_disabling_legacy_live_preview_in_config(self, clean_start):
        create_test_tables(row_count=1)

        op = map(
            command=with_breakpoint("BREAKPOINT; cat"),
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={
                "testing": {
                    "delay_inside_revive": 10000,
                },
            },
            track=False,
        )

        wait(lambda: op.get_state() == "running")

        snapshot_path = op.get_path() + "/snapshot"
        if not clean_start:
            wait(lambda: exists(snapshot_path))

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            set(
                "//sys/controller_agents/config/map_operation_options",
                {"spec_template": {"enable_legacy_live_preview": False}},
            )
            if clean_start and exists(snapshot_path):
                remove(snapshot_path)

        wait(lambda: op.get_state() == "running")

        op.abort()
        wait(lambda: op.get_state() == "aborted")

    @authors("galtsev")
    @pytest.mark.parametrize("clean_start", [False, True])
    def test_do_not_crash_on_enabling_legacy_live_preview_in_config(self, clean_start):
        create_test_tables(row_count=1)

        set(
            "//sys/controller_agents/config/map_reduce_operation_options",
            {"spec_template": {"enable_legacy_live_preview": False}},
        )

        op = map_reduce(
            mapper_command="cat",
            reducer_command=with_breakpoint("BREAKPOINT; cat"),
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            sort_by=["x"],
            spec={
                "testing": {
                    "delay_inside_revive": 10000,
                },
            },
            track=False,
        )

        wait(lambda: op.get_state() == "running")

        snapshot_path = op.get_path() + "/snapshot"
        if not clean_start:
            wait(lambda: exists(snapshot_path))

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            set("//sys/controller_agents/config/map_reduce_operation_options", {})
            if clean_start and exists(snapshot_path):
                remove(snapshot_path)

        wait(lambda: op.get_state() == "running")

        op.abort()
        wait(lambda: op.get_state() == "aborted")
