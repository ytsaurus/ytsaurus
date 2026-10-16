from yt_env_setup import Restarter, NODES_SERVICE, ROOTFS_LAYER_PATH

from yt_commands import (
    create, write_file, wait, get, update_controller_agent_config,
    sync_create_cells, wait_for_cells
)

import yt.environment.init_operations_archive as init_operations_archive


def make_gpu_check_layer_cache_config():
    return {
        "volume_manager": {
            "enable_layers_cache": True,
            # Recipe nodes may share one tmpfs, so bound each node's cache.
            "layer_locations": [{"quota": 512 * 1024 ** 2}],
        },
    }


class GpuCheckBase(object):
    def setup_gpu_layer_and_reset_nodes(self):
        create("map_node", "//tmp/gpu_check")

        create("file", "//tmp/gpu_check/0", attributes={"replication_factor": 1})
        file_name = "layers/gpu_check.tar.gz"
        write_file(
            "//tmp/gpu_check/0",
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
                "layer_paths": ["//tmp/gpu_check/0", ROOTFS_LAYER_PATH],
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
