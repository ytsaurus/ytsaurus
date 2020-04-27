import pytest

from yt_env_setup import YTEnvSetup, wait, Restarter, SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE
from yt_commands import *
from yt_helpers import *

from yt.test_helpers import are_almost_equal
import yt.environment.init_operation_archive as init_operation_archive

from flaky import flaky

import time

import __builtin__

##################################################################

def get_from_tree_orchid(tree, path, **kwargs):
    return get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{}/{}".format(tree, path), **kwargs)

##################################################################

class TestPoolTreesReconfiguration(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            # Unrecognized alert often interferes with the alerts that
            # are tested in this test suite.
            "enable_unrecognized_alert": False,
            "alerts_update_period": 100
        }
    }

    def teardown_method(self, method):
        super(TestPoolTreesReconfiguration, self).teardown_method(method)
        wait(lambda: not get("//sys/scheduler/@alerts"))

    def create_custom_pool_tree_with_one_node(self, pool_tree):
        tag = pool_tree
        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/" + node + "/@user_tags/end", tag)
        set("//sys/pool_trees/default/@nodes_filter", "!" + tag)
        create_pool_tree(pool_tree, attributes={"nodes_filter": tag})
        wait(lambda: tag in get("//sys/scheduler/orchid/scheduler/nodes/{}/tags".format(node)))
        wait(lambda: pool_tree in ls("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"))
        return node


    @authors("asaitgalin")
    def test_basic_sanity(self):
        wait(lambda: exists(scheduler_orchid_default_pool_tree_path()))

        create_pool_tree("other", attributes={"nodes_filter": "other"})

        wait(lambda: exists("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/other/fair_share_info"))
        wait(lambda: not get("//sys/scheduler/@alerts"))

        # This tree intersects with default pool tree by nodes, should not be added
        create_pool_tree("other_intersecting", wait_for_orchid=False, attributes={"nodes_filter": ""})
        wait(lambda: not exists("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/other_intersecting/fair_share_info"))
        wait(lambda: get("//sys/scheduler/@alerts"))

        remove_pool_tree("other_intersecting", wait_for_orchid=False)
        wait(lambda: "update_pools" not in get("//sys/scheduler/@alerts"))

    @authors("asaitgalin")
    def test_abort_orphaned_operations(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])
        create("table", "//tmp/t_out")

        op = map(
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "testing": {"delay_inside_abort": 100},
            },
            track=False)

        wait(lambda: op.get_state() == "running")

        remove("//sys/pool_trees/@default_tree")
        remove_pool_tree("default")

        wait(lambda: op.get_state() in ["aborted", "aborting"])

    @authors("ignat")
    def test_abort_many_orphaned_operations_with_abort(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])

        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{}/@resource_limits_overrides".format(node), {"cpu": 10, "user_slots": 10})

        ops = []
        for i in xrange(10):
            create("table", "//tmp/t_out" + str(i))
            ops.append(map(
                command="sleep 1000; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(i),
                track=False))

        for op in ops:
            wait(lambda: op.get_state() == "running")

        remove("//sys/pool_trees/@default_tree")
        remove_pool_tree("default")

        for op in reversed(ops):
            try:
                op.abort()
            except YtError:
                pass

        for op in ops:
            wait(lambda: op.get_state() in ["aborted", "aborting"])

    @authors("ignat")
    def test_abort_many_orphaned_operations_with_update_runtime_parameters(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])

        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{}/@resource_limits_overrides".format(node), {"cpu": 10, "user_slots": 10})

        ops = []
        for i in xrange(10):
            create("table", "//tmp/t_out" + str(i))
            ops.append(map(
                command="sleep 1000; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(i),
                spec={
                    "testing": {"delay_inside_abort": 100},
                },
                track=False))

        for op in ops:
            wait(lambda: op.get_state() == "running")

        remove("//sys/pool_trees/@default_tree")
        remove("//sys/pool_trees/default")

        for op in reversed(ops):
            try:
                update_op_parameters(op.id, parameters={
                    "scheduling_options_per_pool_tree": {
                        "other1": {
                            "resource_limits": {
                                "user_slots": 1
                            }
                        }
                    }
                })
            except YtError:
                pass

        for op in ops:
            wait(lambda: op.get_state() in ["aborted", "aborting"])

    @authors("ignat")
    def test_abort_many_orphaned_operations_with_multiple_trees(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])

        set("//sys/pool_trees/default/@nodes_filter", "!other1 & !other2 & !other3")

        nodes = ls("//sys/cluster_nodes")
        for index, node in enumerate(nodes):
            tag = "other" + str(index + 1)
            set("//sys/cluster_nodes/{}/@resource_limits_overrides".format(node), {"cpu": 10, "user_slots": 10})
            set("//sys/cluster_nodes/{}/@user_tags".format(node), [tag])
            wait(lambda: tag in get("//sys/scheduler/orchid/scheduler/nodes/{}/tags".format(node)))

            create_pool_tree(tag, attributes={"nodes_filter": tag})
            wait(lambda: tag in ls("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"))

        ops1 = []
        for i in xrange(10):
            create("table", "//tmp/t_out" + str(i))
            ops1.append(map(
                command="sleep 1000; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(i),
                spec={
                    "pool_trees": ["other1"],
                    "testing": {"delay_inside_abort": 100},
                },
                track=False))
        ops12 = []
        for i in xrange(10, 20):
            create("table", "//tmp/t_out" + str(i))
            ops12.append(map(
                command="sleep 1000; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(i),
                pool_trees=["other1", "other2"],
                track=False))
        ops123 = []
        for i in xrange(20, 30):
            create("table", "//tmp/t_out" + str(i))
            ops123.append(map(
                command="sleep 1000; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(i),
                pool_trees=["other1", "other2", "other3"],
                track=False))

        for op in ops1 + ops12 + ops123:
            wait(lambda: op.get_state() == "running")

        remove("//sys/pool_trees/other1")

        for op in ops1:
            wait(lambda: op.get_state() in ["aborted", "aborting"])

        for op in ops12 + ops123:
            assert op.get_state() == "running"


    @authors("asaitgalin")
    def test_multitree_operations(self):
        create("table", "//tmp/t_in")
        for i in xrange(15):
            write_table("<append=%true>//tmp/t_in", [{"x": i}])
        create("table", "//tmp/t_out")

        self.create_custom_pool_tree_with_one_node(pool_tree="other")

        op = map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool_trees": ["default", "other"]},
            track=False)

        op.track()

    @authors("asaitgalin")
    def test_revive_multitree_operation(self):
        create("table", "//tmp/t_in")
        for i in xrange(6):
            write_table("<append=%true>//tmp/t_in", [{"x": i}])
        create("table", "//tmp/t_out")

        self.create_custom_pool_tree_with_one_node(pool_tree="other")

        op = map(
            command="sleep 4; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool_trees": ["default", "other"], "data_size_per_job": 1},
            track=False)

        wait(lambda: len(op.get_running_jobs()) > 2)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            time.sleep(0.5)

        wait(lambda: op.get_state() == "running")
        op.track()

    @authors("asaitgalin", "ignat")
    def test_incorrect_node_tags(self):
        create_pool_tree("supertree1", attributes={"nodes_filter": "x|y"})
        create_pool_tree("supertree2", attributes={"nodes_filter": "y|z"})
        wait(lambda: "supertree1" in ls("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"))
        wait(lambda: "supertree2" in ls("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"))

        node = ls("//sys/cluster_nodes")[0]
        assert get("//sys/scheduler/orchid/scheduler/nodes/" + node + "/scheduler_state") == "online"
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 3

        assert not get("//sys/scheduler/@alerts")

        set("//sys/cluster_nodes/" + node + "/@user_tags/end", "y")

        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/" + node + "/scheduler_state") == "offline")
        wait(lambda: get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 2)
        wait(lambda: get("//sys/scheduler/@alerts"))
        assert get("//sys/scheduler/@alerts")[0]

    @authors("asaitgalin")
    def test_default_tree_manipulations(self):
        assert get("//sys/pool_trees/@default_tree") == "default"

        remove("//sys/pool_trees/@default_tree")
        wait(lambda: not exists(scheduler_orchid_path() + "/scheduler/default_pool_tree"))

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])
        create("table", "//tmp/t_out")

        try:
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out")
            assert False, "Didn't throw"
        except YtError as e:
            assert e.contains_code(214)  # NScheduler::EErrorCode::PoolTreesAreUnspecified

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"pool_trees": ["default"]})

        set("//sys/pool_trees/@default_tree", "unexisting")
        wait(lambda: get("//sys/scheduler/@alerts"))
        wait(lambda: not exists("//sys/scheduler/orchid/scheduler/default_fair_share_tree"))

        set("//sys/pool_trees/@default_tree", "default")
        wait(lambda: exists("//sys/scheduler/orchid/scheduler/default_fair_share_tree"))
        assert get("//sys/scheduler/orchid/scheduler/default_fair_share_tree") == "default"

    @authors("asaitgalin")
    def test_fair_share(self):
        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=%true>//tmp/t_in", [{"x": i}])

        node = self.create_custom_pool_tree_with_one_node(pool_tree="other")

        orchid_root = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"
        wait(lambda: get(orchid_root + "/default/node_count") == 2)
        wait(lambda: get(orchid_root + "/other/node_count") == 1)
        assert are_almost_equal(get(orchid_root + "/default/resource_limits")["cpu"], 2)
        assert are_almost_equal(get(orchid_root + "/other/resource_limits")["cpu"], 1)
        assert node in get(orchid_root + "/other/node_addresses")
        assert node not in get(orchid_root + "/default/node_addresses")

        create("table", "//tmp/t_out_1")
        op1 = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_1",
            spec={"pool_trees": ["default", "other"], "data_size_per_job": 1},
            track=False)

        create("table", "//tmp/t_out_2")
        op2 = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_2",
            spec={"pool_trees": ["other"], "data_size_per_job": 1},
            track=False)

        def get_fair_share(tree, op_id):
            try:
                return get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{0}/fair_share_info/operations/{1}/fair_share_ratio"
                           .format(tree, op_id))
            except YtError:
                return 0.0

        wait(lambda: are_almost_equal(get_fair_share("default", op1.id), 1.0))
        wait(lambda: are_almost_equal(get_fair_share("other", op1.id), 0.5))
        wait(lambda: are_almost_equal(get_fair_share("other", op2.id), 0.5))

    @authors("asaitgalin", "shakurov")
    def test_default_tree_update(self):
        self.create_custom_pool_tree_with_one_node(pool_tree="other")
        time.sleep(0.5)

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])

        create("table", "//tmp/t_out_1")
        op1 = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_1",
            track=False)
        wait(lambda: op1.get_state() == "running")

        set("//sys/pool_trees/@default_tree", "other")
        wait(lambda: get("//sys/scheduler/orchid/scheduler/default_fair_share_tree") == "other")
        assert op1.get_state() == "running"

        create("table", "//tmp/t_out_2")
        op2 = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_2",
            track=False)
        wait(lambda: op2.get_state() == "running")

        operations_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{}/fair_share_info/operations"
        default_operations_path = operations_path.format("default")
        other_operations_path = operations_path.format("other")

        wait(lambda: len(ls(default_operations_path)) == 1)
        wait(lambda: len(ls(other_operations_path)) == 1)

        default_tree_operations = get(default_operations_path)
        other_tree_operations = get(other_operations_path)
        assert op1.id in default_tree_operations
        assert op1.id not in other_tree_operations
        assert op2.id in other_tree_operations
        assert op2.id not in default_tree_operations

    @authors("ignat")
    def test_node_tags_changed(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1} for iter in xrange(10)])

        create_pool_tree("other", attributes={"nodes_filter": "other"})
        set("//sys/pool_trees/default/@nodes_filter", "!other")
        wait(lambda: "other" in ls("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"))

        nodes = ls("//sys/cluster_nodes")
        set("//sys/cluster_nodes/" + nodes[0] + "/@user_tags", ["other"])
        wait(lambda: "other" in get("//sys/scheduler/orchid/scheduler/nodes/{}/tags".format(nodes[0])))

        create("table", "//tmp/t_out1")
        op1 = map(
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out1",
            spec={
                "pool_trees": ["default"],
                "job_count": 10,
            },
            track=False)
        wait(lambda: op1.get_job_count("running") == 2)

        create("table", "//tmp/t_out2")
        op2 = map(
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out2",
            spec={
                "pool_trees": ["other"],
                "job_count": 10,
            },
            track=False)
        wait(lambda: op2.get_job_count("running") == 1)

        set("//sys/cluster_nodes/" + nodes[1] + "/@user_tags", ["other"])
        wait(lambda: "other" in get("//sys/scheduler/orchid/scheduler/nodes/{}/tags".format(nodes[1])))

        wait(lambda: op1.get_job_count("running") == 1)
        wait(lambda: op1.get_job_count("aborted") == 1)
        wait(lambda: op2.get_job_count("running") == 2)

    @authors("renadeen")
    def test_race_between_pool_tree_removal_and_register_operation(self):
        # Scenario:
        # 1. operation is running
        # 2. user updates node_filter of pool tree
        # 3. scheduler removes and adds that tree
        # 4. scheduler unregisters and aborts all operations of removed tree before publishing new trees
        # 5. abort of operation causes fiber switch
        # 6. new operation registers in old tree that is being removed
        # 7. all aborts are completed, scheduler publishes new tree structure (without new operation)
        # 8. operation tries to complete scheduler doesn't know this operation and crashes
        # NB(ignat): This scenario is not valid anymore since update pool trees is atomic now, but abort is asynchronous.

        node = self.create_custom_pool_tree_with_one_node(pool_tree="other")
        orchid_root = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"
        set("//sys/pool_trees/@default_tree", "other")

        run_test_vanilla(
            "sleep 1000",
            job_count=1,
            spec={"testing": {"delay_inside_abort": 3000}}
        )

        remove("//sys/pool_trees/other")
        set("//sys/pool_trees/@default_tree", "default")
        wait(lambda: "other" not in ls(orchid_root))

        create_pool_tree("other", attributes={"nodes_filter": "other"})
        set("//sys/pool_trees/@default_tree", "other")
        # We actually wait abort here since previous update can actually finish only after abort of all operations.
        wait(lambda: "other" in ls(orchid_root))
        run_test_vanilla(":", job_count=1)

@authors("renadeen")
class TestConfigurablePoolTreeRoot(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 0

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "pool_trees_root": "//sys/test_root"
        }
    }

    def test_scheduler_reads_pool_config_from_different_path(self):
        set("//sys/test_root", {
            "tree": {
                "parent": {"pool": {}}
            }
        })
        set("//sys/test_root/tree/parent/pool/@max_operation_count", 10)

        pools_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/tree/fair_share_info/pools"
        wait(lambda: exists(pools_path + "/pool"))
        wait(lambda: get(pools_path + "/pool/parent") == "parent")
        wait(lambda: get(pools_path + "/pool/max_operation_count") == 10)

##################################################################

class TestTentativePoolTrees(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 1

    TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT = 5
    MAX_TENTATIVE_TREE_JOB_DURATION_RATIO = 2
    TENTATIVE_TREE_ELIGIBILITY_MIN_JOB_DURATION = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "orchid_keys_update_period": 100,
            "static_orchid_cache_update_period": 100
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "check_tentative_tree_eligibility_period": 1000000,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent" : {
            "job_controller" : {
                "resource_limits" : {
                    "user_slots" : 2,
                    "cpu" : 2,
                },
            },
        },
    }

    def setup_method(self, method):
        super(TestTentativePoolTrees, self).setup_method(method)
        set("//sys/controller_agents/config/check_tentative_tree_eligibility_period", 100 * 1000)

    # Creates and additional pool tree called "other", configures tag filters,
    # tags some nodes as "other" and returns a list of those nodes.
    def _prepare_pool_trees(self):
        other_nodes = ls("//sys/cluster_nodes")[:3]
        for node in other_nodes:
            set("//sys/cluster_nodes/" + node + "/@user_tags/end", "other")

        set("//sys/pool_trees/default/@nodes_filter", "!other")
        create_pool_tree("other", attributes={"nodes_filter": "other"})

        return other_nodes

    def _create_spec(self):
        return {
            "pool_trees": ["default"],
            "tentative_pool_trees": ["other"],
            "scheduling_options_per_pool_tree": {
                "default": {
                    "min_share_resources": {"cpu": 1}
                },
                "other": {
                    "pool": "superpool"
                }
            },
            "tentative_tree_eligibility": {
                "sample_job_count": TestTentativePoolTrees.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT,
                "max_tentative_job_duration_ratio": TestTentativePoolTrees.MAX_TENTATIVE_TREE_JOB_DURATION_RATIO,
                "min_job_duration": TestTentativePoolTrees.TENTATIVE_TREE_ELIGIBILITY_MIN_JOB_DURATION,
            },
            "data_size_per_job": 1,
        }

    def _iter_running_jobs(self, op, tentative_nodes):
        jobs_path = op.get_path() + "/controller_orchid/running_jobs"

        try:
            jobs = ls(jobs_path)
        except YtError:
            return []

        result = []
        for job_id in jobs:
            try:
                job_node = get("{0}/{1}".format(jobs_path, job_id))["address"]
            except YtError:
                continue # The job has already completed, Orchid is lagging.

            job_is_tentative = job_node in tentative_nodes
            result.append((job_id, job_is_tentative))
        return result

    # It's just flapping sheet YT-11156
    @flaky(max_runs=5)
    @authors("ignat")
    def test_tentative_pool_tree_sampling(self):
        other_nodes = self._prepare_pool_trees()
        spec = self._create_spec()

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(20)])
        create("table", "//tmp/t_out")

        op = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=spec,
            track=False)

        jobs_path = op.get_path() + "/controller_orchid/running_jobs"

        dummy = {"jobs": [], "stability_count": 0} # no "nonlocal" support in python 2
        def all_jobs_running():
            try:
                old_job_count = len(dummy["jobs"])
                dummy["jobs"] = ls(jobs_path)
                new_job_count = len(dummy["jobs"])
                if new_job_count == old_job_count:
                    dummy["stability_count"] += 1

                return dummy["stability_count"] > 5
            except:
                return False

        wait(all_jobs_running)

        tentative_job_count = 0
        for job_id in dummy["jobs"]:
            job_node = get("{0}/{1}".format(jobs_path, job_id))["address"]
            if job_node in other_nodes:
                tentative_job_count += 1

        assert tentative_job_count == TestTentativePoolTrees.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT

        # Check that tentative tree saturated and we have proper deactivation reasons about that.
        orchid_other_operations_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/other/fair_share_info/operations"
        assert get("{}/{}/deactivation_reasons/saturated_in_tentative_tree".format(orchid_other_operations_path, op.id)) > 0

    @authors("ignat")
    def test_tentative_pool_tree_not_supported(self):
        self._prepare_pool_trees()
        spec = self._create_spec()

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(30)])
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        events = events_on_fs()

        op2 = map_reduce(
            mapper_command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
            reducer_command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
            sort_by=["x"],
            in_="//tmp/t_in",
            out="//tmp/t_out1",
            spec=spec,
            track=False)

        op1 = map(
            command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
            in_="//tmp/t_in",
            out="//tmp/t_out2",
            spec=spec,
            track=False)


        op1_pool_trees_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/".format(op1.id)
        op2_pool_trees_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/".format(op2.id)

        wait(lambda: exists(op1_pool_trees_path + "default"))
        wait(lambda: exists(op1_pool_trees_path + "other"))
        wait(lambda: exists(op2_pool_trees_path + "default"))
        wait(lambda: not exists(op2_pool_trees_path + "other"))

    @authors("ignat")
    def test_tentative_pool_tree_banning(self):
        other_node_list = self._prepare_pool_trees()
        other_nodes = frozenset(other_node_list)

        spec = self._create_spec()

        set("//sys/controller_agents/config", {"check_tentative_tree_eligibility_period": 500})

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(30)])
        create("table", "//tmp/t_out")

        events = events_on_fs()

        op = map(
            command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=spec,
            track=False)

        op_pool_trees_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/".format(op.id)

        def operations_failed_or_aborted():
            return op.get_state() in ["failed", "aborted"]

        def has_all_tentative_jobs():
            assert not operations_failed_or_aborted()
            tentative_job_count = 0
            for job_id, tentative in self._iter_running_jobs(op, other_nodes):
                if tentative:
                    tentative_job_count += 1
            return tentative_job_count == TestTentativePoolTrees.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT

        wait(has_all_tentative_jobs)

        # Sleep to make job durations long enonugh.
        time.sleep(5)

        wait(lambda: exists(op_pool_trees_path + "other"))

        def complete_non_tentative_jobs(context):
            assert not operations_failed_or_aborted()
            for job_id, tentative in self._iter_running_jobs(op, other_nodes):
                if not tentative and job_id not in context["completed_jobs"] and len(context["completed_jobs"]) < 20:
                    print_debug("Complete job {0}".format(job_id))
                    context["completed_jobs"].add(job_id)
                    events.notify_event("continue_job_{0}".format(job_id))
            return len(context["completed_jobs"]) == 20

        # We have 30 jobs overall, 5 should be tentative, 20 regular jobs we complete fast. It must be enough to ban tentative tree.
        context = {"completed_jobs": __builtin__.set()}
        wait(lambda: complete_non_tentative_jobs(context))

        wait(lambda: op.get_job_count("completed") == 20)

        op_pool_trees_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/".format(op.id)
        wait(lambda: not exists(op_pool_trees_path + "other"))
        assert exists(op_pool_trees_path + "default")

    @authors("ignat")
    def test_missing_tentative_pool_trees(self):
        self._prepare_pool_trees()
        spec = self._create_spec()

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(7)])
        create("table", "//tmp/t_out")

        spec["tentative_pool_trees"] = ["missing"]
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec=spec)

        spec["tentative_tree_eligibility"]["ignore_missing_pool_trees"] = True
        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec=spec)

    @authors("ignat")
    def test_tentative_pool_tree_aborted_jobs(self):
        other_node_list = self._prepare_pool_trees()
        other_nodes = frozenset(other_node_list)

        spec = self._create_spec()

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(30)])
        create("table", "//tmp/t_out")

        events = events_on_fs()

        op = map(
            command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=spec,
            track=False)

        job_aborted = False
        for iter in xrange(20):
            time.sleep(0.5)

            for job_id, tentative in self._iter_running_jobs(op, other_nodes):
                if tentative:
                    try:
                        abort_job(job_id)
                        job_aborted = True
                        break
                    # Job can be published by controller agent but still be missing in scheduler.
                    except YtError:
                        pass

            if job_aborted:
                break

        assert job_aborted

        tentative_job_count = 0
        while tentative_job_count + 1 < TestTentativePoolTrees.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT:
            time.sleep(0.5)
            for job_id, tentative in self._iter_running_jobs(op, other_nodes):
                if tentative:
                    events.notify_event("continue_job_{0}".format(job_id))
                    tentative_job_count += 1

    @authors("ignat")
    def test_use_default_tentative_pool_trees(self):
        other_node_list = self._prepare_pool_trees()

        set("//sys/scheduler/config/default_tentative_pool_trees", ["other"], recursive=True)
        wait(lambda: exists("//sys/scheduler/orchid/scheduler/config/default_tentative_pool_trees"))

        spec = self._create_spec()
        spec["use_default_tentative_pool_trees"] = True

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(30)])
        create("table", "//tmp/t_out")

        events = events_on_fs()
        op = map(
            command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=spec,
            track=False)

        op_pool_trees_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/".format(op.id)
        wait(lambda: exists(op_pool_trees_path + "other"))
        assert get(op_pool_trees_path + "other/tentative")

class TestSchedulingTagFilterOnPerPoolTreeConfiguration(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "spec_template": {
                "scheduling_options_per_pool_tree": {
                    "default": {"scheduling_tag_filter": "default_tag"},
                    "custom_pool_tree": {"scheduling_tag_filter": "runnable_tag"}
                }
            }
        }
    }

    @authors("renadeen")
    def test_scheduling_tag_filter_applies_from_per_pool_tree_config(self):
        all_nodes = ls("//sys/cluster_nodes")
        default_node = all_nodes[0]
        custom_node = all_nodes[1]
        runnable_custom_node = all_nodes[2]
        set("//sys/cluster_nodes/" + default_node + "/@user_tags/end", "default_tag")
        set("//sys/cluster_nodes/" + custom_node + "/@user_tags/end", "custom_tag")
        set("//sys/cluster_nodes/" + runnable_custom_node + "/@user_tags", ["custom_tag", "runnable_tag"])

        set("//sys/pool_trees/default/@nodes_filter", "default_tag")
        create_pool_tree("custom_pool_tree", attributes={"nodes_filter": "custom_tag"})

        create_test_tables()

        op = map(
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool_trees": ["custom_pool_tree"]},
            track=False)

        wait_breakpoint()

        jobs = op.get_running_jobs()
        assert len(jobs) == 1
        assert jobs[jobs.keys()[0]]["address"] == runnable_custom_node

        release_breakpoint()

##################################################################

class TestSchedulerScheduleInSingleTree(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 1000,
            "fair_share_profiling_period": 100,
            "operations_update_period": 10,
            "operations_cleaner": {
                "enable": False,
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operations_update_period": 100,
            "controller_static_orchid_update_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 100  # 100 msec
            },
            "job_proxy_heartbeat_period": 100,  # 100 msec
            "job_controller": {
                "resource_limits": {
                    "cpu": 3,
                    "user_slots": 3
                }
            }
        },
    }

    def setup_method(self, method):
        super(TestSchedulerScheduleInSingleTree, self).setup_method(method)

        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client(), override_tablet_cell_bundle="default")

        nodes = ls("//sys/cluster_nodes")
        for node, tag in zip(nodes, ["default_tag", "nirvana_tag", "cloud_tag"]):
            set("//sys/cluster_nodes/{}/@user_tags".format(node), [tag])

        set("//sys/pool_trees/default/@nodes_filter", "default_tag")
        set("//sys/pool_trees/default/@total_resource_limits_consider_delay", 1000)
        pool_orchid = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{}/fair_share_info/pools/{}"
        for tree in ["default", "nirvana", "cloud"]:
            if tree != "default":
                create_pool_tree(tree, attributes={"nodes_filter": tree + "_tag",
                                                   "total_resource_limits_consider_delay": 1000})
            create_pool("research", pool_tree=tree)
            # Create "prodX" pools to spread guaranteed resources ratio.
            for i in range(9):
                create_pool("prod" + str(i), pool_tree=tree)
            wait(lambda: exists(pool_orchid.format(tree, "research"), verbose_error=True))
            for i in range(9):
                wait(lambda: exists(pool_orchid.format(tree, "prod" + str(i)), verbose_error=True))

    def _get_tree_for_job(self, job):
        node = job["address"]
        tag = get("//sys/cluster_nodes/" + node + "/@user_tags")[0]
        assert tag.endswith("_tag")
        tree = tag[:-4]
        print_debug("Job {} was scheduled in tree {} (node {})".format(job["id"], tree, node))
        return tree

    def _check_tree_for_operation_jobs(self, op, possible_trees, expected_job_count=None):
        if expected_job_count is not None:
            wait(lambda: len(list_jobs(op.id)["jobs"]) >= expected_job_count)
        jobs = list_jobs(op.id)["jobs"]
        op_tree = self._get_tree_for_job(jobs[0])
        assert op_tree in possible_trees
        for job in jobs[1:]:
            assert self._get_tree_for_job(job) == op_tree
        return op_tree

    def _run_vanilla_and_check_tree(self, spec, possible_trees, job_count=10):
        op = run_test_vanilla("sleep 0.6", job_count=job_count, spec=spec, track=True)
        wait(lambda: len(list_jobs(op.id)["jobs"]) >= job_count)
        erased_trees = get(op.get_path() + "/@erased_trees")

        op_tree = self._check_tree_for_operation_jobs(op, possible_trees, job_count)
        spec_trees = spec["pool_trees"] if "pool_trees" in spec else ["default"]
        assert op_tree not in erased_trees
        assert (frozenset(erased_trees) | {op_tree}) == frozenset(spec_trees)

    @authors("eshcherbin")
    def test_one_empty_tree(self):
        spec = {
            "pool_trees": ["nirvana"],
            "pool": "research",
            "schedule_in_single_tree": True
        }
        possible_trees = [
            "nirvana"
        ]
        self._run_vanilla_and_check_tree(spec, possible_trees)

    @authors("eshcherbin")
    def test_one_empty_tree_ephemeral(self):
        spec = {
            "schedule_in_single_tree": True
        }
        possible_trees = [
            "default"
        ]
        self._run_vanilla_and_check_tree(spec, possible_trees)

    @authors("eshcherbin")
    def test_two_empty_trees(self):
        spec = {
            "pool_trees": ["nirvana", "cloud"],
            "pool": "research",
            "schedule_in_single_tree": True
        }
        possible_trees = [
            "nirvana",
            "cloud"
        ]
        self._run_vanilla_and_check_tree(spec, possible_trees)

    @authors("eshcherbin")
    def test_two_trees_with_unequal_demand(self):
        for busy_tree, expected_tree in [("default", "nirvana"), ("nirvana", "default")]:
            wait(lambda: get_from_tree_orchid(expected_tree, "fair_share_info/pools/research/resource_demand/cpu") == 0.0)
            other_op = run_sleeping_vanilla(spec={"pool_trees": [busy_tree], "pool": "research"})
            wait(lambda: get_from_tree_orchid(busy_tree, "fair_share_info/pools/research/resource_demand/cpu") > 0.0)

            spec = {
                "pool_trees": ["default", "nirvana"],
                "pool": "research",
                "schedule_in_single_tree": True
            }
            possible_trees = [
                expected_tree
            ]
            self._run_vanilla_and_check_tree(spec, possible_trees)

            other_op.abort()
            other_op.wait_for_state("aborted")

    @authors("eshcherbin")
    def test_two_trees_with_unequal_min_share_resources(self):
        for other_tree, expected_tree in [("default", "nirvana"), ("nirvana", "default")]:
            set("//sys/pool_trees/{}/research/@min_share_resources".format(expected_tree), {"cpu": 1})
            wait(lambda: get_from_tree_orchid(expected_tree, "fair_share_info/pools/research/min_share_resources/cpu") == 1.0)

            spec = {
                "pool_trees": ["default", "nirvana"],
                "pool": "research",
                "schedule_in_single_tree": True
            }
            possible_trees = [
                expected_tree
            ]
            self._run_vanilla_and_check_tree(spec, possible_trees)

            set("//sys/pool_trees/{}/research/@min_share_resources".format(expected_tree), {"cpu": 0})

    @authors("eshcherbin")
    def test_two_trees_with_unequal_total_resources(self):
        spare_node = ls("//sys/cluster_nodes")[2]

        for other_tree, expected_tree in [("default", "nirvana"), ("nirvana", "default")]:
            set("//sys/cluster_nodes/{}/@user_tags".format(spare_node), [expected_tree + "_tag"])
            wait(lambda: get_from_tree_orchid(expected_tree, "fair_share_info/pools/research/resource_limits/cpu") == 6.0)

            spec = {
                "pool_trees": ["default", "nirvana"],
                "pool": "research",
                "schedule_in_single_tree": True
            }
            possible_trees = [
                expected_tree
            ]
            self._run_vanilla_and_check_tree(spec, possible_trees)

            set("//sys/cluster_nodes/{}/@user_tags".format(spare_node), [])

    @authors("eshcherbin")
    def test_prefer_tree_with_min_share_resources(self):
        set("//sys/pool_trees/nirvana/research/@min_share_resources", {"cpu": 3})
        wait(lambda: get_from_tree_orchid("nirvana", "fair_share_info/pools/research/min_share_resources/cpu") == 3.0)
        other_op = run_sleeping_vanilla(spec={"pool_trees": ["nirvana"], "pool": "research"}, job_count=2)

        spec = {
            "pool_trees": ["default", "nirvana"],
            "pool": "research",
            "schedule_in_single_tree": True
        }
        possible_trees = [
            "nirvana"
        ]
        self._run_vanilla_and_check_tree(spec, possible_trees, job_count=3)

        other_op.abort()
        other_op.wait_for_state("aborted")
        set("//sys/pool_trees/nirvana/research/@min_share_resources", {"cpu": 0})

    @authors("eshcherbin")
    def test_revive_scheduler(self):
        job_count = 10
        possible_trees = ["default", "nirvana", "cloud"]
        spec = {
            "pool_trees": possible_trees,
            "pool": "research",
            "schedule_in_single_tree": True
        }

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=job_count, spec=spec)
        wait_breakpoint()

        op.wait_fresh_snapshot()
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            erased_trees = get(op.get_path() + "/@erased_trees")

        wait(lambda: get("//sys/scheduler/orchid/scheduler/operations/{}/state".format(op.id), verbose_error=False) == "running")
        op.wait_for_state("running")
        wait(lambda: get(op.get_path() + "/@erased_trees") == erased_trees)

        release_breakpoint()
        op.track()

        op_tree = self._check_tree_for_operation_jobs(op, possible_trees, job_count)
        assert op_tree not in erased_trees
        assert (frozenset(erased_trees) | {op_tree}) == frozenset(possible_trees)

    @authors("eshcherbin")
    def test_revive_controller_agent(self):
        job_count = 10
        possible_trees = ["default", "nirvana", "cloud"]
        spec = {
            "pool_trees": possible_trees,
            "pool": "research",
            "schedule_in_single_tree": True
        }

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=job_count, spec=spec)
        wait_breakpoint()

        op.wait_fresh_snapshot()
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            erased_trees = get(op.get_path() + "/@erased_trees")
            assert len(possible_trees) == len(erased_trees) + 1
            for tree in erased_trees:
                set("//sys/pool_trees/{}/research/@min_share_resources".format(tree), {"cpu": 3})
                wait(lambda: get_from_tree_orchid(tree, "fair_share_info/pools/research/min_share_resources/cpu") == 3.0)

        wait(lambda: get("//sys/scheduler/orchid/scheduler/operations/{}/state".format(op.id), verbose_error=False) == "running")
        op.wait_for_state("running")
        wait(lambda: get(op.get_path() + "/@erased_trees") == erased_trees)

        release_breakpoint()
        op.track()

        op_tree = self._check_tree_for_operation_jobs(op, possible_trees, job_count)
        assert op_tree not in erased_trees
        assert (frozenset(erased_trees) | {op_tree}) == frozenset(possible_trees)

        for tree in erased_trees:
            set("//sys/pool_trees/{}/research/@min_share_resources".format(tree), {"cpu": 0})

    @authors("eshcherbin")
    def test_ignore_trees_where_operation_is_not_running(self):
        for tree in ["default", "nirvana"]:
            set("//sys/pool_trees/{}/research/@max_running_operation_count".format(tree), 1)
            wait(lambda: get_from_tree_orchid(tree, "fair_share_info/pools/research/max_running_operation_count") == 1)

        for busy_tree, expected_tree in [("default", "nirvana"), ("nirvana", "default")]:
            wait(lambda: get_from_tree_orchid(expected_tree, "fair_share_info/pools/research/resource_demand/cpu") == 0.0)
            other_op = run_sleeping_vanilla(spec={"pool_trees": [busy_tree], "pool": "research"})
            wait(lambda: get_from_tree_orchid(busy_tree, "fair_share_info/pools/research/resource_demand/cpu") > 0.0)

            set("//sys/pool_trees/{}/research/@min_share_resources".format(busy_tree), {"cpu": 3})
            wait(lambda: get_from_tree_orchid(busy_tree, "fair_share_info/pools/research/min_share_resources/cpu") == 3.0)

            spec = {
                "pool_trees": ["default", "nirvana"],
                "pool": "research",
                "schedule_in_single_tree": True
            }
            possible_trees = [
                expected_tree
            ]
            self._run_vanilla_and_check_tree(spec, possible_trees)

            other_op.abort()
            other_op.wait_for_state("aborted")
            set("//sys/pool_trees/{}/research/@min_share_resources".format(busy_tree), {"cpu": 0})

        for tree in ["default", "nirvana"]:
            set("//sys/pool_trees/{}/research/@max_running_operation_count".format(tree), 8)
            wait(lambda: get_from_tree_orchid(tree, "fair_share_info/pools/research/max_running_operation_count") == 8)

    @authors("eshcherbin")
    def test_global_disable(self):
        set("//sys/scheduler/config/enable_schedule_in_single_tree", False)
        wait(lambda: not get(scheduler_orchid_path() + "/scheduler/config/enable_schedule_in_single_tree"))

        job_count = 10
        possible_trees = ["default", "nirvana", "cloud"]
        spec = {
            "pool_trees": possible_trees,
            "pool": "research",
            "schedule_in_single_tree": True
        }

        op = run_test_vanilla("sleep 0.6", job_count=job_count, spec=spec, track=True)
        wait(lambda: len(list_jobs(op.id)["jobs"]) >= job_count)
        wait(lambda: get(op.get_path() + "/@erased_trees") == [])

    @authors("eshcherbin")
    def test_global_enable_during_operation_materialization(self):
        set("//sys/scheduler/config/enable_schedule_in_single_tree", False)
        wait(lambda: not get(scheduler_orchid_path() + "/scheduler/config/enable_schedule_in_single_tree"))
        set("//sys/scheduler/config/fair_share_update_period", 5000)
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/config/fair_share_update_period") == 5000)

        # TODO(eshcherbin): Remove this sleep in favour of a more stable way to do the same wait.
        # This sleep is used to ensure the last fair share update before the fair share update period change has finished.
        time.sleep(1.0)

        job_count = 10
        possible_trees = ["default", "nirvana", "cloud"]
        spec = {
            "pool_trees": possible_trees,
            "pool": "research",
            "schedule_in_single_tree": True,
            "testing": {
                "delay_inside_materialize": 1500
            }
        }

        op = run_test_vanilla("sleep 0.6", job_count=job_count, spec=spec, track=False)
        op.wait_for_state("materializing")

        set("//sys/scheduler/config/enable_schedule_in_single_tree", True)
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/config/enable_schedule_in_single_tree"))

        # Introduce this not to fail due to poor timings.
        # Should always be true because of the delay above.
        option_disabled_during_materialization = (op.get_state() == "materializing")
        if not option_disabled_during_materialization:
            print_debug("Warning: could not disable \"schedule_in_single_tree\" during "
                        "materialization, probably due to poor timings.")

        op.track()
        if option_disabled_during_materialization:
            wait(lambda: get(op.get_path() + "/@erased_trees") == [])

##################################################################

class TestPoolTreeOperationLimits(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "static_orchid_cache_update_period": 100
        }
    }

    @authors("eshcherbin")
    def test_enabling_operation_separately_in_each_tree(self):
        nodes = ls("//sys/cluster_nodes")
        for node in nodes[:-1]:
            set("//sys/cluster_nodes/{0}/@user_tags".format(node), ["other"])
        set("//sys/pool_trees/default/@nodes_filter", "!other")
        create_pool_tree("other",
                         attributes={"nodes_filter": "other", "max_running_operation_count_per_pool": 1},
                         wait_for_orchid=True)

        blocking_op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={"pool_trees": ["other"]})
        wait_breakpoint()

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=2, spec={"pool_trees": ["default", "other"]})

        time.sleep(3)
        # NB(eshcherbin): There are 2 nodes in tree "other" and 1 node in tree "default".
        # Both trees have "max_running_operation_count_per_pool" set to 1.
        # At this moment operation `blocking_op` is running in tree "other", so operation `op` can only run in tree "default",
        # where it can have only 1 out of the 2 desired jobs.
        # Thus, if everything is correct, op should be enabled only in tree "default".
        assert op.get_job_count("running") == 1

        release_breakpoint()
        blocking_op.track()
        op.track()

    @authors("mrkastep")
    def test_ignoring_tentative_pool_operation_limit(self):
        nodes = ls("//sys/cluster_nodes")
        for normal_node in nodes[:2]:
            set("//sys/cluster_nodes/{0}/@user_tags".format(normal_node), ["normal"])
        for tentative_node in nodes[2:]:
            set("//sys/cluster_nodes/{0}/@user_tags".format(tentative_node), ["tentative"])

        set("//sys/pool_trees/default/@nodes_filter", "!(normal|tentative)")
        create_pool_tree("normal", attributes={"nodes_filter": "normal"})
        create_pool("pool", pool_tree="normal", attributes={"max_operation_count": 5})
        create_pool_tree("tentative", attributes={"nodes_filter": "tentative"})
        create_pool("pool", pool_tree="tentative", attributes={"max_operation_count": 3})

        pool_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{}/fair_share_info/pools/pool"
        wait(lambda: exists(pool_path.format("normal")))
        wait(lambda: exists(pool_path.format("tentative")))

        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        for i in xrange(6):
            create("table", "//tmp/out" + str(i))

        ops = []
        def run(index, trees, tentative_trees, should_raise):
            def execute(track):
                return map(
                    track=track,
                    command="sleep 1000; cat",
                    in_=["//tmp/in"],
                    out="//tmp/out" + str(index),
                    spec={
                        "pool_trees": list(trees),
                        "tentative_pool_trees": list(tentative_trees),
                        "scheduling_options_per_pool_tree": {tree : {"pool" : "pool"} for tree in trees | tentative_trees}
                    })

            if should_raise:
                with pytest.raises(YtError):
                    execute(track=True)
            else:
                op = execute(track=False)
                wait(lambda: op.get_state() in ("pending", "running"))
                ops.append(op)

        for i in xrange(3):
            run(i, {"normal", "tentative"}, frozenset(), False)

        for i in xrange(3, 5):
            run(i, {"normal", "tentative"}, frozenset(), True)

        for i in xrange(3, 5):
            run(i, {"normal"}, {"tentative"}, False)

        for i in xrange(5, 6):
            run(i, {"normal"}, {"tentative"}, True)

        wait(lambda: get(pool_path.format("normal") + "/operation_count") == 5)
        wait(lambda: get(pool_path.format("tentative") + "/operation_count") == 3)

        for op in ops:
            op.abort()

        set("//sys/pool_trees/default/@nodes_filter", "")

##################################################################

class TestTreeSetChangedDuringFairShareUpdate(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 0

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 1000,
            "strategy_testing_options": {
                "delay_inside_fair_share_update": 900
            }
        }
    }

    @authors("eshcherbin")
    def test_tree_set_changed_during_fair_share_update(self):
        for i in range(10):
            create_pool_tree("other{}".format(i), wait_for_orchid=True)
            # This sleep is needed here to spread the creation of new pool trees over time.
            time.sleep(0.6)

        for i in range(10):
            remove_pool_tree("other{}".format(i), wait_for_orchid=True)
            # This sleep is needed here to spread the deletion of new pool trees over time.
            time.sleep(0.6)
