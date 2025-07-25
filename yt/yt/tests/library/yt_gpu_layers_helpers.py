from yt_env_setup import Restarter, NODES_SERVICE

from yt_commands import (
    create, write_file, wait, get, update_controller_agent_config,
    sync_create_cells, wait_for_cells
)

import yt.environment.init_operations_archive as init_operations_archive


class GpuCheckBase(object):
    def setup_gpu_layer_and_reset_nodes(self, prepare_gpu_base_layer=False):
        create("map_node", "//tmp/gpu_check")

        create("file", "//tmp/gpu_check/0", attributes={"replication_factor": 1})
        file_name = "layers/gpu_check.tar.gz"
        write_file(
            "//tmp/gpu_check/0",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

        create("file", "//tmp/base_layer", attributes={"replication_factor": 1})
        file_name = "rootfs/rootfs.tar.gz"
        write_file(
            "//tmp/base_layer",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

        if prepare_gpu_base_layer:
            # Using same layer for root volume and GPU check volume could causes job aborts.
            #
            # Explanation: using same layer for these two volume could cause job abort with error
            # 'Cannot find a suitable location for artifact chunk' while GPU check volume preparation.
            # The root cause of this abort is following: exec node cannot download chunk in cache if the chunk is in removing state in cache.
            # And we face this situation in case when the node tries to download chunk the second time for prepare the same layer second time.
            #
            # Note that layer cache can help to avoid this problem, but it is disabled because of another issue, see detailed in
            # `yt/python/yt/environment/default_config.py`.
            create("file", "//tmp/gpu_base_layer", attributes={"replication_factor": 1})
            file_name = "rootfs/rootfs.tar.gz"
            write_file(
                "//tmp/gpu_base_layer",
                open(file_name, "rb").read(),
                file_writer={"upload_replication_factor": 1},
            )

        # Reload node to reset alerts.
        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait(lambda: list(get("//sys/scheduler/orchid/scheduler/nodes").values())[0]["resource_limits"]["user_slots"] > 0)
        wait_for_cells()

    def setup_tables(self):
        create(
            "table",
            "//tmp/t_in",
            attributes={"replication_factor": 1},
        )
        create(
            "table",
            "//tmp/t_out",
            attributes={"replication_factor": 1},
        )

    def setup_gpu_check_options(self, binary_path="/gpu_check/gpu_check_success", binary_args=None):
        update_controller_agent_config(
            "operation_options/gpu_check",
            {
                "layer_paths": ["//tmp/gpu_check/0", "//tmp/gpu_base_layer"],
                "binary_path": binary_path,
                "binary_args": binary_args if binary_args is not None else [],
            }
        )

    def init_operations_archive(self):
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )
