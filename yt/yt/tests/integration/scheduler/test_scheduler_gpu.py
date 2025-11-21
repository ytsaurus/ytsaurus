import datetime
import time

from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    NODES_SERVICE,
)

from yt_commands import (
    authors, create, wait, write_table, ls, get, set, create_data_center, create_rack, run_sleeping_vanilla, update_pool_tree_config,
    update_pool_tree_config_option, create_pool_tree, exists, map, update_scheduler_config,
)

from yt_scheduler_helpers import (
    scheduler_orchid_path, scheduler_orchid_node_path, scheduler_new_orchid_pool_tree_path,
)


##################################################################


class TestDryRunGpuSchedulingPolicy(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 500,
                    },
                },
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8
                },
            },
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 10,
                "user_slots": 10,
            },
        }
    }

    DATA_CENTER = "SAS"
    RACK = "SAS1"

    def setup_method(self, method):
        super(TestDryRunGpuSchedulingPolicy, self).setup_method(method)

        update_pool_tree_config("default", {"nodes_filter": "!gpu"})
        create_pool_tree("gpu", config={
            "nodes_filter": "gpu",
            "main_resource": "gpu",
            "gpu_scheduling_policy": {
                "mode": "dry_run",
                "plan_update_period": 100,
                "module_type": "data_center",
                "modules": [TestDryRunGpuSchedulingPolicy.DATA_CENTER],
                "full_host_aggressive_preemption_timeout": 1000,
            },
            "preemptive_scheduling_backoff": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_starvation_tolerance": 0.95,
            "preemption_satisfaction_threshold": 0.99,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
        })

        set("//sys/pool_trees/@default_tree", "gpu")
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/default_pool_tree", default=None) == "gpu")

        create_data_center(TestDryRunGpuSchedulingPolicy.DATA_CENTER)
        create_rack(TestDryRunGpuSchedulingPolicy.RACK)
        set("//sys/racks/{}/@data_center".format(TestDryRunGpuSchedulingPolicy.RACK), TestDryRunGpuSchedulingPolicy.DATA_CENTER)
        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@rack".format(node), TestDryRunGpuSchedulingPolicy.RACK)
            set("//sys/cluster_nodes/{}/@user_tags".format(node), ["gpu"])
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == TestDryRunGpuSchedulingPolicy.DATA_CENTER)

        wait(lambda: get(scheduler_new_orchid_pool_tree_path("gpu") + "/node_count") == TestDryRunGpuSchedulingPolicy.NUM_NODES)

    def _get_operation_from_orchid(self, operation, tree="gpu"):
        return get(scheduler_new_orchid_pool_tree_path(tree) + f"/gpu_assignment_plan/operations/{operation.id}")

    def _get_operation_assignments_from_orchid(self, operation, tree="gpu"):
        return self._get_operation_from_orchid(operation, tree=tree)["assignments"]

    def _check_assignment(self, assignment, operation_id, group_name, gpu_usage):
        assert assignment["operation_id"] == operation_id
        assert assignment["allocation_group_name"] == group_name
        assert assignment["resource_usage"]["gpu"] == gpu_usage

    def _check_operation(self, operation, is_gang, group_name, allocation_count, min_needed_gpu_per_allocation, assigned_gpu_usage, assignments_count, enabled=None):
        assert operation["gang"] == is_gang
        assert group_name in operation["initial_grouped_needed_resources"]
        assert operation["initial_grouped_needed_resources"][group_name]["allocation_count"] == allocation_count
        assert operation["initial_grouped_needed_resources"][group_name]["min_needed_resources"]["gpu"] == min_needed_gpu_per_allocation
        assert operation["assigned_resource_usage"]["gpu"] == assigned_gpu_usage
        assert len(operation["assignments"]) == assignments_count
        if enabled is not None:
            assert operation["enabled"] == enabled

    def _wait_for_operations_in_orchid(self, operations_count, tree="gpu"):
        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path(tree) + "/gpu_assignment_plan/operations")) == operations_count)

    @authors("eshcherbin")
    def test_simple(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait(lambda: exists(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan"))

        self._wait_for_operations_in_orchid(operations_count=1)

        operation = self._get_operation_from_orchid(op)
        self._check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=1,
            assigned_gpu_usage=1,
            assignments_count=1,
            enabled=True,
        )
        assignment = operation["assignments"][0]
        self._check_assignment(assignment, op.id, "task", 1)

        node_address = assignment["node_address"]
        assert node_address in ls("//sys/cluster_nodes")
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert node["assigned_resource_usage"]["gpu"] == 1
        assert node["resourse_limits"]["gpu"] == 8
        assert node["scheduling_module"] == TestDryRunGpuSchedulingPolicy.DATA_CENTER

        assert len(get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")) == 1
        assert get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")[0] == assignment

    @authors("yaishenka")
    def test_simple_full_host(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        self._wait_for_operations_in_orchid(operations_count=1)

        operation = self._get_operation_from_orchid(op)
        self._check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=8,
            assignments_count=1,
            enabled=True,
        )

        assignment = operation["assignments"][0]
        self._check_assignment(assignment, op.id, "task", 8)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 1
        assert node["assignments"][0] == assignment

    @authors("yaishenka")
    def test_simple_two_jobs(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            job_count=2,
        )

        wait(lambda: len(op.get_running_jobs()) == 2)

        self._wait_for_operations_in_orchid(operations_count=1)

        operation = self._get_operation_from_orchid(op)
        self._check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=2,
            min_needed_gpu_per_allocation=1,
            assigned_gpu_usage=2,
            assignments_count=2,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            self._check_assignment(assignment, op.id, "task", 1)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 2
        for assignment in node["assignments"]:
            self._check_assignment(assignment, op.id, "task", 1)

    @authors("yaishenka")
    def test_simple_two_jobs_full_host(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=2,
        )

        wait(lambda: len(op.get_running_jobs()) == 2)

        self._wait_for_operations_in_orchid(operations_count=1)

        operation = self._get_operation_from_orchid(op)
        self._check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=2,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=16,
            assignments_count=2,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            self._check_assignment(assignment, op.id, "task", 8)

            node_address = assignment["node_address"]
            node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
            assert len(node["assignments"]) == 1
            for node_assignment in node["assignments"]:
                assert node_assignment == assignment

    @authors("yaishenka")
    def test_simple_two_ops(self):
        op1 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            job_count=1,
        )
        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait(lambda: len(op2.get_running_jobs()) == 1)

        self._wait_for_operations_in_orchid(operations_count=2)

        for op in [op1, op2]:
            operation = self._get_operation_from_orchid(op)
            self._check_operation(
                operation=operation,
                is_gang=False,
                group_name="task",
                allocation_count=1,
                min_needed_gpu_per_allocation=1,
                assigned_gpu_usage=1,
                assignments_count=1,
                enabled=True,
            )

            for assignment in operation["assignments"]:
                self._check_assignment(assignment, op.id, "task", 1)

    # Just test that in theory it also works with CPU trees.
    @authors("eshcherbin")
    def test_simple_cpu_tree(self):
        update_pool_tree_config("default", {"nodes_filter": "!gpu & !cpu"})
        create_pool_tree("cpu", config={
            "nodes_filter": "cpu",
            "gpu_scheduling_policy": {
                "mode": "dry_run",
                "plan_update_period": 100,
                "module_type": "data_center",
                "modules": [TestDryRunGpuSchedulingPolicy.DATA_CENTER],
                "full_host_aggressive_preemption_timeout": 1000,
            },
            "preemptive_scheduling_backoff": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_starvation_tolerance": 0.95,
            "preemption_satisfaction_threshold": 0.99,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
        })

        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@user_tags".format(node), ["cpu"])
        wait(lambda: get(scheduler_new_orchid_pool_tree_path("cpu") + "/node_count") == TestDryRunGpuSchedulingPolicy.NUM_NODES)

        op = run_sleeping_vanilla(spec={"pool_trees": ["cpu"]})

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait(lambda: exists(scheduler_new_orchid_pool_tree_path("cpu") + "/gpu_assignment_plan"))

        self._wait_for_operations_in_orchid(operations_count=1, tree="cpu")

        operation = self._get_operation_from_orchid(op, tree="cpu")
        self._check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=0,
            assigned_gpu_usage=0,
            assignments_count=1,
            enabled=True,
        )
        assignment = operation["assignments"][0]
        self._check_assignment(assignment, op.id, "task", gpu_usage=0)

        node_address = assignment["node_address"]
        assert node_address in ls("//sys/cluster_nodes")
        node = get(scheduler_new_orchid_pool_tree_path("cpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert node["assigned_resource_usage"]["cpu"] == 1.0
        assert node["resourse_limits"]["cpu"] == 10.0
        assert node["scheduling_module"] == TestDryRunGpuSchedulingPolicy.DATA_CENTER

        assert len(get(scheduler_new_orchid_pool_tree_path("cpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")) == 1
        assert get(scheduler_new_orchid_pool_tree_path("cpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")[0] == assignment

    @authors("yaishenka")
    def test_two_ops_full_host(self):
        op1 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
        )
        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait(lambda: len(op2.get_running_jobs()) == 1)

        self._wait_for_operations_in_orchid(operations_count=2)

        for op in [op1, op2]:
            operation = self._get_operation_from_orchid(op)
            self._check_operation(
                operation=operation,
                is_gang=False,
                group_name="task",
                allocation_count=1,
                min_needed_gpu_per_allocation=8,
                assigned_gpu_usage=8,
                assignments_count=1,
                enabled=True,
            )

            for assignment in operation["assignments"]:
                self._check_assignment(assignment, op.id, "task", 8)

    @authors("yaishenka")
    def test_vanilla_more_gpu_goes_first(self):
        op1 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            job_count=1,
            track=False,
            spec={"testing": {"delay_inside_materialize": 50}},
        )
        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=1,
            track=False
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait(lambda: len(op2.get_running_jobs()) == 1)

        self._wait_for_operations_in_orchid(operations_count=2)

        assignment1 = self._get_operation_assignments_from_orchid(op1)[0]
        assignment2 = self._get_operation_assignments_from_orchid(op2)[0]

        op1_creation_time = datetime.datetime.fromisoformat(assignment1["creation_time"])
        op2_creation_time = datetime.datetime.fromisoformat(assignment2["creation_time"])

        assert op2_creation_time <= op1_creation_time

    @authors("yaishenka")
    def test_vanilla_gang(self):
        update_pool_tree_config_option("gpu", "enable_step_function_for_gang_operations", False)
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"is_gang": True},
            job_count=2,
        )

        wait(lambda: len(op.get_running_jobs()) == 2)

        self._wait_for_operations_in_orchid(operations_count=1)

        operation = self._get_operation_from_orchid(op)
        self._check_operation(
            operation=operation,
            is_gang=True,
            group_name="task",
            allocation_count=2,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=16,
            assignments_count=2,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            self._check_assignment(assignment, op.id, "task", 8)

            node_address = assignment["node_address"]
            node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
            assert len(node["assignments"]) == 1
            for node_assignment in node["assignments"]:
                assert node_assignment == assignment

    @authors("yaishenka")
    def test_operation_cant_schedule(self):
        run_sleeping_vanilla(
            task_patch={"gpu_limit": 9, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: exists(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan"))

        time.sleep(10)
        assert len(get(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan/operations")) == 0

    @authors("yaishenka")
    def test_simple_map(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1000",
            spec={
                "mapper": {
                    "job_count": 1,
                    "gpu_limit": 1,
                    "enable_gpu_layers": False,
                },
            },
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        self._wait_for_operations_in_orchid(operations_count=1)

        operation = self._get_operation_from_orchid(op)
        self._check_operation(
            operation=operation,
            is_gang=False,
            group_name="map",
            allocation_count=1,
            min_needed_gpu_per_allocation=1,
            assigned_gpu_usage=1,
            assignments_count=1,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            self._check_assignment(assignment, op.id, "map", 1)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 1
        for assignment in node["assignments"]:
            self._check_assignment(assignment, op.id, "map", 1)

    @authors("yaishenka")
    def test_simple_fullhost_map(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1000",
            spec={
                "mapper": {
                    "job_count": 1,
                    "gpu_limit": 8,
                    "enable_gpu_layers": False,
                },
            },
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        self._wait_for_operations_in_orchid(operations_count=1)

        operation = self._get_operation_from_orchid(op)
        self._check_operation(
            operation=operation,
            is_gang=False,
            group_name="map",
            allocation_count=1,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=8,
            assignments_count=1,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            self._check_assignment(assignment, op.id, "map", 8)

    @authors("yaishenka")
    def test_fullhost_map_two_jobs(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        for _ in range(2):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1000",
            spec={
                "job_count": 2,
                "mapper": {
                    "gpu_limit": 8,
                    "enable_gpu_layers": False,
                },
            }
        )

        wait(lambda: len(op.get_running_jobs()) == 2)
        self._wait_for_operations_in_orchid(operations_count=1)

        operation = self._get_operation_from_orchid(op)
        self._check_operation(
            operation=operation,
            is_gang=False,
            group_name="map",
            allocation_count=2,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=16,
            assignments_count=2,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            self._check_assignment(assignment, op.id, "map", 8)

    @authors("yaishenka")
    def test_vanilla_goes_first(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op1 = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1000",
            spec={
                "mapper": {
                    "job_count": 1,
                    "gpu_limit": 1,
                    "enable_gpu_layers": False,
                },
                "testing": {
                    "delay_inside_materialize": 50,
                },
            },
        )
        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait(lambda: len(op2.get_running_jobs()) == 1)

        self._wait_for_operations_in_orchid(operations_count=2)

        assignment1 = self._get_operation_assignments_from_orchid(op1)[0]
        assignment2 = self._get_operation_assignments_from_orchid(op2)[0]

        op1_creation_time = datetime.datetime.fromisoformat(assignment1["creation_time"])
        op2_creation_time = datetime.datetime.fromisoformat(assignment2["creation_time"])

        assert op2_creation_time <= op1_creation_time

    @authors("eshcherbin")
    def test_orchid_with_offline_node(self):
        update_scheduler_config("node_registration_timeout", 1000)

        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan/nodes")) ==
             TestDryRunGpuSchedulingPolicy.NUM_NODES)
        with Restarter(self.Env, NODES_SERVICE):
            wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan/nodes")) == 0)

            update_scheduler_config("testing_options/node_heartbeat_processing_delay", {
                "duration": 3000,
                "type": "async",
            })

        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan/nodes")) ==
             TestDryRunGpuSchedulingPolicy.NUM_NODES)


##################################################################
