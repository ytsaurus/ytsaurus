from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, with_breakpoint, wait_breakpoint, release_breakpoint,
    create, get, ls, write_table,
    get_data_nodes, get_exec_nodes, get_tablet_nodes,
    run_test_vanilla, sync_create_cells)

#################################################################


class TestNodeFlavors(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @classmethod
    def modify_node_config(cls, config):
        node_flavors = [
            ["data"],
            ["exec"],
            ["tablet"],
            ["data", "exec"],
            ["data", "tablet"],
        ]

        if not hasattr(cls, "node_counter"):
            cls.node_counter = 0
        config["flavors"] = node_flavors[cls.node_counter]
        cls.node_counter = (cls.node_counter + 1) % cls.NUM_NODES

    @authors("gritukan")
    def test_data_nodes(self):
        # TODO(gritukan): Disable store locations for exec nodes.
        data_nodes = get_data_nodes() + get_exec_nodes()
        assert len(data_nodes) == 5

        create("table", "//tmp/t")
        for i in range(5):
            write_table("//tmp/t", [{"x": i}])
        for chunk_id in get("//tmp/t/@chunk_ids"):
            for replica in get("#{}/@stored_replicas".format(chunk_id)):
                assert str(replica) in data_nodes

    @authors("gritukan")
    def test_exec_nodes(self):
        exec_nodes = get_exec_nodes()
        assert len(exec_nodes) == 2

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=2)
        job_ids = wait_breakpoint(job_count=2)
        for job_id in job_ids:
            get(op.get_path() + "/controller_orchid/running_jobs/{}/address".format(job_id)) in exec_nodes
        release_breakpoint()

    @authors("gritukan")
    def test_tablet_nodes(self):
        tablet_nodes = get_tablet_nodes()
        assert len(tablet_nodes) == 2

        sync_create_cells(2)
        for cell_id in ls("//sys/tablet_cells"):
            peers = get("#" + cell_id + "/@peers")
            assert len(peers) == 1
            assert peers[0]["address"] in tablet_nodes

##################################################################


class TestNodeFlavorsMulticell(TestNodeFlavors):
    NUM_SECONDARY_MASTER_CELLS = 2
