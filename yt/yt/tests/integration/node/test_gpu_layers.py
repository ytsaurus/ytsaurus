from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE

from yt_commands import (
    authors, wait, create, ls, get, set, exists,
    start_transaction, commit_transaction,
    write_file, read_table, write_table,
    map, vanilla, update_nodes_dynamic_config,
    sync_create_cells, get_job, create_pool,
    run_test_vanilla,
    with_breakpoint, wait_breakpoint, release_breakpoint)

import yt.environment.init_operations_archive as init_operations_archive

import pytest

import time
from functools import partial
from collections import Counter


# Increased timeout due to YT-16030
INCREASED_TIMEOUT = 90.0


@authors("ignat")
class TestGpuJobSetup(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "dynamic_config_manager": {
            "enable_unrecognized_options_alert": True,
        },
        "exec_node": {
            "gpu_manager": {
                # For to GPU manager to initialize properly.
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 1,
                    "test_setup_commands": True,
                },
            },
            "job_proxy": {
                "test_root_fs": True,
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "use_artifact_binds": True,
                        "job_setup_command": {
                            "path": "/static-bin/static-bash",
                            "args": ["-c", "echo SETUP-OUTPUT > /setup_output_file"],
                        },
                    },
                },
                "gpu_manager": {
                    "job_setup_command": {
                        "path": "/static-bin/static-bash",
                        "args": [
                            "-c",
                            "echo SETUP-GPU-OUTPUT > /gpu_setup_output_file",
                        ],
                    },
                },
            },
        },
    }

    USE_PORTO = True

    def setup_files(self):
        create("file", "//tmp/layer1", attributes={"replication_factor": 1})
        file_name = "layers/static-bin.tar.gz"
        write_file(
            "//tmp/layer1",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

    @pytest.mark.timeout(180)
    def test_setup_cat(self):
        self.setup_files()

        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/setup_output_file >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/layer1"],
                    "job_count": 1,
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        res = op.read_stderr(job_id)
        assert res == b"SETUP-OUTPUT\n"

        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/gpu_setup_output_file >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/layer1"],
                    "job_count": 1,
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        res = op.read_stderr(job_id)
        assert res == b"SETUP-GPU-OUTPUT\n"

    @authors("eshcherbin")
    @pytest.mark.timeout(180)
    def test_dynamic_config_for_gpu_setup_commands(self):
        self.setup_files()
        update_nodes_dynamic_config({
            "exec_node": {
                "gpu_manager": {
                    "job_setup_command": {
                        "path": "/static-bin/static-bash",
                        "args": [
                            "-c",
                            "echo SETUP-GPU-OUTPUT-DYNAMIC > /gpu_setup_output_file",
                        ],
                    },
                    "cuda_toolkit_min_driver_version": {"1": "0"},
                },
            },
        })

        op = run_test_vanilla(
            "$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/gpu_setup_output_file >&2",
            spec={
                "max_failed_job_count": 1,
            },
            task_patch={
                "layer_paths": ["//tmp/layer1"],
            },
            track=True,
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        res = op.read_stderr(job_id)
        assert res == b"SETUP-GPU-OUTPUT-DYNAMIC\n"


@authors("ignat")
class TestSkipGpuJobSetup(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_root_fs": True,
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "use_artifact_binds": True,
                        "job_setup_command": {
                            "path": "/static-bin/static-bash",
                            "args": ["-c", "echo SETUP-OUTPUT > /setup_output_file"],
                        },
                    },
                },
                "gpu_manager": {
                    "job_setup_command": {
                        "path": "/static-bin/static-bash",
                        "args": ["-c", "echo SETUP-JUNK > /setup_output_file"],
                    },
                },
            },
        },
    }

    USE_PORTO = True

    def setup_files(self):
        create("file", "//tmp/layer1", attributes={"replication_factor": 1})
        file_name = "layers/static-bin.tar.gz"
        write_file(
            "//tmp/layer1",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

    @pytest.mark.timeout(180)
    def test_setup_cat(self):
        self.setup_files()

        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/setup_output_file >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/layer1"],
                    "job_count": 1,
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        res = op.read_stderr(job_id)
        assert res == b"SETUP-OUTPUT\n"


@authors("ignat")
class TestGpuLayer(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1
    NUM_SECONDARY_MASTER_CELLS = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_root_fs": True,
            },
            "gpu_manager": {
                "driver_layer_directory_path": "//tmp/drivers",
                "driver_version": "test_version",
                "testing": {
                    "test_resource": True,
                    "test_layers": True,
                    "test_gpu_count": 1,
                },
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "use_artifact_binds": True,
                        "job_setup_command": {
                            "path": "/static-bin/static-bash",
                            "args": ["-c", "echo SETUP-OUTPUT > /setup_output_file"],
                        },
                    },
                },
                "gpu_manager": {
                    "driver_layer_fetching": {
                        "period": 10000,
                        "splay": 1000,
                    }
                },
            },
        },
    }

    USE_PORTO = True

    def setup_files(self):
        tx = start_transaction()

        create("map_node", "//tmp/drivers", tx=tx)
        create(
            "file",
            "//tmp/drivers/test_version",
            attributes={"replication_factor": 1},
            tx=tx,
        )

        file_name = "layers/static-bin.tar.gz"
        write_file(
            "//tmp/drivers/test_version",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
            tx=tx,
        )

        create("file", "//tmp/layer2", attributes={"replication_factor": 1}, tx=tx)
        file_name = "layers/test.tar.gz"
        write_file(
            "//tmp/layer2",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
            tx=tx,
        )

        commit_transaction(tx)

    @pytest.mark.timeout(180)
    def test_setup_cat_gpu_layer(self):
        self.setup_files()

        get("//tmp/drivers/test_version/@content_revision")

        create(
            "table",
            "//tmp/t_in",
            attributes={"replication_factor": 1},
            file_writer={"upload_replication_factor": 1},
        )
        create(
            "table",
            "//tmp/t_out",
            attributes={"replication_factor": 1},
            file_writer={"upload_replication_factor": 1},
        )

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/setup_output_file >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/layer2"],
                    "enable_gpu_layers": True,
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        res = op.read_stderr(job_id)
        assert res == b"SETUP-OUTPUT\n"


@authors("ignat")
class TestGpuLayerUpdate(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1
    NUM_SECONDARY_MASTER_CELLS = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_root_fs": True,
            },
            "gpu_manager": {
                "driver_layer_directory_path": "//tmp/drivers",
                "driver_version": "test_version",
                "testing": {
                    "test_resource": True,
                    "test_layers": True,
                    "test_gpu_count": 1,
                },
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "use_artifact_binds": True,
                    },
                },
                "gpu_manager": {
                    "driver_layer_fetching": {
                        "period": 10000,
                        "splay": 1000,
                    }
                },
            },
        },
    }

    USE_PORTO = True

    def _write_driver_layer(self, name):
        path = "layers/{}.tar.gz".format(name)
        write_file(
            "//tmp/drivers/test_version",
            open(path, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

    def setup_files(self):
        tx = start_transaction()

        create("map_node", "//tmp/drivers", tx=tx)
        create(
            "file",
            "//tmp/drivers/test_version",
            attributes={"replication_factor": 1},
            tx=tx,
        )

        create("file", "//tmp/bin", attributes={"replication_factor": 1}, tx=tx)
        file_name = "layers/static-bin.tar.gz"
        write_file(
            "//tmp/bin",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
            tx=tx,
        )

        commit_transaction(tx)

    @pytest.mark.timeout(180)
    def test_update_file(self):
        self.setup_files()

        create(
            "table",
            "//tmp/t_in",
            attributes={"replication_factor": 1},
            file_writer={"upload_replication_factor": 1},
        )
        create(
            "table",
            "//tmp/t_out",
            attributes={"replication_factor": 1},
            file_writer={"upload_replication_factor": 1},
        )

        write_table("//tmp/t_in", [{"k": 0}])

        def check_cat(content):
            op = map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/name >&2",
                spec={
                    "max_failed_job_count": 1,
                    "mapper": {
                        "job_count": 1,
                        "layer_paths": ["//tmp/bin"],
                        "enable_gpu_layers": True,
                    },
                },
            )

            job_ids = op.list_jobs()
            assert len(job_ids) == 1
            job_id = job_ids[0]

            res = op.read_stderr(job_id).decode("ascii")
            return res == content

        self._write_driver_layer("olli")
        wait(partial(check_cat, "Olli Tukiainen\n"), ignore_exceptions=True)

        self._write_driver_layer("marko")
        wait(partial(check_cat, "Marko Saaresto\n"), ignore_exceptions=True)


@authors("ignat")
class TestCudaLayer(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1
    NUM_SECONDARY_MASTER_CELLS = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_root_fs": True,
            },
            "gpu_manager": {
                "driver_version": "0",
                "testing": {
                    "test_resource": True,
                    "test_layers": True,
                    "test_gpu_count": 1,
                },
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "use_artifact_binds": True,
                        "job_setup_command": {
                            "path": "/static-bin/static-bash",
                            "args": ["-c", "echo SETUP-OUTPUT > /setup_output_file"],
                        },
                    },
                },
                "gpu_manager": {
                    "cuda_toolkit_min_driver_version": {"0": "0"},
                },
            },
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"cuda_toolkit_layer_directory_path": "//tmp/cuda"}}

    USE_PORTO = True

    def setup_files(self, cuda_version="0"):
        create("map_node", "//tmp/cuda")

        create("file", "//tmp/cuda/" + cuda_version, attributes={"replication_factor": 1})
        file_name = "layers/static-bin.tar.gz"
        write_file(
            "//tmp/cuda/" + cuda_version,
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

        create("file", "//tmp/layer2", attributes={"replication_factor": 1})
        file_name = "layers/test.tar.gz"
        write_file(
            "//tmp/layer2",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

    @pytest.mark.timeout(180)
    def test_setup_cat_gpu_layer(self):
        self.setup_files()

        create(
            "table",
            "//tmp/t_in",
            attributes={"replication_factor": 1},
            file_writer={"upload_replication_factor": 1},
        )
        create(
            "table",
            "//tmp/t_out",
            attributes={"replication_factor": 1},
            file_writer={"upload_replication_factor": 1},
        )

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/setup_output_file >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/layer2"],
                    "enable_gpu_layers": True,
                    "cuda_toolkit_version": "0",
                    "gpu_limit": 1,
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        res = op.read_stderr(job_id)
        assert res == b"SETUP-OUTPUT\n"

    @pytest.mark.timeout(180)
    def test_dynamic_config_for_cuda_toolkit_version(self):
        self.setup_files(cuda_version="1")
        update_nodes_dynamic_config({
            "exec_node": {
                "gpu_manager": {
                    "cuda_toolkit_min_driver_version": {"1": "0"},
                },
            },
        })

        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        write_table("//tmp/t_in", [{"k": 0}])

        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/setup_output_file >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/layer2"],
                    "enable_gpu_layers": True,
                    "cuda_toolkit_version": "1",
                    "gpu_limit": 1,
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        res = op.read_stderr(job_id)
        assert res == b"SETUP-OUTPUT\n"


@authors("ignat")
class TestForceCudaLayer(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1
    NUM_SECONDARY_MASTER_CELLS = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_root_fs": True,
            },
            "gpu_manager": {
                "driver_version": "0",
                "driver_layer_directory_path": "//tmp/drivers",
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 1,
                },
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "use_artifact_binds": True,
                    },
                },
                "gpu_manager": {
                    "cuda_toolkit_min_driver_version": {"0": "0"},
                    "job_setup_command": {
                        "path": "/static-bin/static-bash",
                        "args": [
                            "-c",
                            "echo SETUP-OUTPUT > /playground/setup_output_file",
                        ],
                    },
                },
            },
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"cuda_toolkit_layer_directory_path": "//tmp/cuda"}}

    USE_PORTO = True

    def setup_files(self):
        create("map_node", "//tmp/cuda")
        create("map_node", "//tmp/drivers")

        create("file", "//tmp/cuda/0", attributes={"replication_factor": 1})
        file_name = "layers/static-bin.tar.gz"
        write_file(
            "//tmp/cuda/0",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

        create("file", "//tmp/layer2", attributes={"replication_factor": 1})
        file_name = "layers/test.tar.gz"
        write_file(
            "//tmp/layer2",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

        create("file", "//tmp/drivers/0", attributes={"replication_factor": 1})
        file_name = "layers/playground.tar.gz"
        write_file(
            "//tmp/drivers/0",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

    @pytest.mark.timeout(180)
    def test_setup_cat_force_gpu_layer(self):
        self.setup_files()
        with Restarter(self.Env, NODES_SERVICE):
            pass

        create(
            "table",
            "//tmp/t_in",
            attributes={"replication_factor": 1},
            file_writer={"upload_replication_factor": 1},
        )
        create(
            "table",
            "//tmp/t_out",
            attributes={"replication_factor": 1},
            file_writer={"upload_replication_factor": 1},
        )

        write_table("//tmp/t_in", [{"k": 0}])

        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/playground/setup_output_file >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/layer2"],
                    "enable_gpu_layers": True,
                    "cuda_toolkit_version": "0",
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        res = op.read_stderr(job_id)
        assert res == b"SETUP-OUTPUT\n"


@authors("omgronny")
class TestCudaProfilerLayer(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1
    NUM_SECONDARY_MASTER_CELLS = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_root_fs": True,
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"cuda_profiler_layer_path": "//tmp/cuda-profiler"}}

    USE_PORTO = True

    def setup_files(self):
        create("file", "//tmp/cuda-profiler", attributes={"replication_factor": 1})
        file_name = "layers/cupti-injection-libs.tar.gz"
        write_file(
            "//tmp/cuda-profiler",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

    def test_setup_cuda_profiler_layer(self):
        self.setup_files()

        command = """
            if [[ ! -f "$YT_ROOT_FS/opt/cupti-lib/libcupti_trace_injection.so" ]];
            then exit 1;
            fi
        """

        task_spec = {
            "command": command,
            "profilers": [{
                "binary": "user_job",
                "type": "cuda",
                "profiling_probability": 1,
            }],
            "job_count": 1,
        }

        op = vanilla(spec={
            "tasks": {
                "task": task_spec,
            },
        })
        op.wait_for_state("completed")


@authors("ignat")
class TestSetupUser(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_root_fs": True,
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "use_artifact_binds": True,
                        "job_setup_command": {
                            "path": "/static-bin/static-bash",
                            "args": [
                                "-c",
                                "/static-bin/static-id -u > /playground/setup_output_file",
                            ],
                        },
                        "setup_command_user": "2019",
                    },
                },
            },
        }
    }
    USE_PORTO = True

    def setup_files(self):
        create("file", "//tmp/layer1", attributes={"replication_factor": 1})
        file_name = "layers/static-bin.tar.gz"
        write_file(
            "//tmp/layer1",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

        create("file", "//tmp/playground_layer", attributes={"replication_factor": 1})
        file_name = "layers/playground.tar.gz"
        write_file(
            "//tmp/playground_layer",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

    @pytest.mark.timeout(180)
    def test_setup_cat(self):
        self.setup_files()

        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/playground/setup_output_file >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/layer1", "//tmp/playground_layer"],
                    "job_count": 1,
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        res = op.read_stderr(job_id)
        assert res == b"2019\n"


class TestRootFS(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    USE_PORTO = True
    USE_CUSTOM_ROOTFS = True

    @authors("gritukan")
    @pytest.mark.timeout(180)
    def test_map(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": 1}])

        op = map(track=False, in_="//tmp/t_in", out="//tmp/t_out", command="static_cat")
        op.track()

        assert read_table("//tmp/t_out") == [{"x": 1}]

    @authors("gritukan")
    @pytest.mark.timeout(180)
    def test_vanilla(self):
        create("table", "//tmp/stderr")

        op = vanilla(
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": 'echo "task_a" >&2',
                    },
                    "task_b": {
                        "job_count": 1,
                        "command": 'echo "task_b" >&2',
                    },
                },
                "stderr_table_path": "//tmp/stderr",
            }
        )

        table_stderrs = read_table("//tmp/stderr")
        table_stderrs_per_task = Counter(row["data"] for row in table_stderrs)

        job_ids = op.list_jobs()
        cypress_stderrs_per_task = Counter(op.read_stderr(job_id).decode("ascii") for job_id in job_ids)

        assert dict(table_stderrs_per_task) == {"task_a\n": 1, "task_b\n": 1}
        assert dict(cypress_stderrs_per_task) == {"task_a\n": 1, "task_b\n": 1}


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

        create("file", "//tmp/base_layer", attributes={"replication_factor": 1})
        file_name = "rootfs/rootfs.tar.gz"
        write_file(
            "//tmp/base_layer",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

        # Reload node to reset alerts.
        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait(lambda: list(get("//sys/scheduler/orchid/scheduler/nodes").values())[0]["resource_limits"]["user_slots"] > 0)

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


@authors("ignat")
class TestGpuCheck(YTEnvSetup, GpuCheckBase):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    USE_DYNAMIC_TABLES = True
    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_root_fs": True,
            },
            "gpu_manager": {
                "driver_version": "0",
                "testing": {
                    "test_resource": True,
                    "test_layers": True,
                    "test_gpu_count": 1,
                },
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "use_artifact_binds": True,
                    },
                },
                "job_reporter": {
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                },
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "gpu_check_layer_directory_path": "//tmp/gpu_check"
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        }
    }

    @pytest.mark.timeout(180)
    def test_gpu_check_success(self):
        self.setup_gpu_layer_and_reset_nodes()

        self.setup_tables()

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo AAA >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/base_layer"],
                    "enable_gpu_layers": True,
                    "gpu_check_layer_name": "0",
                    "gpu_check_binary_path": "/gpu_check/gpu_check_success",
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        res = op.read_stderr(job_id)
        assert res == b"AAA\n"

    @pytest.mark.timeout(180)
    def test_gpu_check_success_with_failed_job(self):
        self.setup_gpu_layer_and_reset_nodes()

        self.setup_tables()

        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo AAA >&2; exit 1",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/base_layer"],
                    "enable_gpu_layers": True,
                    "gpu_check_layer_name": "0",
                    "gpu_check_binary_path": "/gpu_check/gpu_check_success",
                },
            },
            track=False,
        )

        wait(lambda: op.get_state() == "failed", timeout=INCREASED_TIMEOUT)

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        events = get_job(op.id, job_id)["events"]
        phases = [event["phase"] for event in events if "phase" in event]
        assert "running_extra_gpu_check_command" in phases

    @pytest.mark.timeout(180)
    def test_gpu_check_fail(self):
        self.setup_gpu_layer_and_reset_nodes()

        self.setup_tables()

        node = ls("//sys/cluster_nodes")[0]

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo AAA >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/base_layer"],
                    "enable_gpu_layers": True,
                    "gpu_check_layer_name": "0",
                    "gpu_check_binary_path": "/gpu_check/gpu_check_fail",
                },
            },
        )

        alerts_path = "//sys/cluster_nodes/{}/@alerts".format(node)
        wait(lambda: get(alerts_path), timeout=INCREASED_TIMEOUT)

        alerts = get(alerts_path)
        assert len(alerts) == 1
        assert "GPU check command failed" in str(alerts[0])

        resource_limits_path = "//sys/cluster_nodes/{}/@resource_limits".format(node)
        wait(lambda: get(resource_limits_path)["user_slots"] == 0)

        wait(lambda: op.get_state() == "failed")

    @pytest.mark.timeout(180)
    def test_gpu_check_missing(self):
        self.setup_gpu_layer_and_reset_nodes()

        self.setup_tables()

        node = ls("//sys/cluster_nodes")[0]

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo AAA >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/base_layer"],
                    "enable_gpu_layers": True,
                    "gpu_check_layer_name": "0",
                    "gpu_check_binary_path": "/gpu_check/gpu_check_missing",
                },
            },
        )

        wait(lambda: op.get_state() == "failed", timeout=INCREASED_TIMEOUT)

        alerts_path = "//sys/cluster_nodes/{}/@alerts".format(node)
        assert len(get(alerts_path)) == 0

    @pytest.mark.timeout(180)
    def test_disable_jobs_on_gpu_check_failure(self):
        self.setup_gpu_layer_and_reset_nodes()

        self.setup_tables()

        config = {
            "%true": {
                "exec_node": {
                    "slot_manager": {
                        "disable_jobs_on_gpu_check_failure": False
                    }
                }
            }
        }
        set("//sys/cluster_nodes/@config", config)

        node = ls("//sys/cluster_nodes")[0]

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo AAA >&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/base_layer"],
                    "enable_gpu_layers": True,
                    "gpu_check_layer_name": "0",
                    "gpu_check_binary_path": "/gpu_check/gpu_check_fail",
                },
            },
        )

        alerts_path = "//sys/cluster_nodes/{}/@alerts".format(node)
        wait(lambda: get(alerts_path), timeout=INCREASED_TIMEOUT)

        alerts = get(alerts_path)
        assert len(alerts) == 1
        assert "GPU check command failed" in str(alerts[0])

        time.sleep(2.0)

        assert op.get_state() == "running"

    @pytest.mark.timeout(180)
    def test_gpu_check_abort(self):
        self.setup_gpu_layer_and_reset_nodes()

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        def gpu_check_is_running(job_id):
            node_orchid_job_path = "//sys/cluster_nodes/{}/orchid/exec_node/job_controller/active_jobs/{}"\
                                   .format(node, job_id)
            if not exists(node_orchid_job_path):
                return False
            events = get(node_orchid_job_path + "/events")
            phases = [event["phase"] for event in events if "phase" in event]
            return phases[-1] == "running_gpu_check_command"

        create(
            "table",
            "//tmp/t_in",
            attributes={"replication_factor": 1},
        )
        for i in (1, 2):
            create(
                "table",
                "//tmp/t_out" + str(i),
                attributes={"replication_factor": 1},
            )
        write_table("//tmp/t_in", [{"k": 0}])

        set("//sys/pool_trees/default/@config/main_resource", "gpu")
        set("//sys/pool_trees/default/@config/max_unpreemptible_running_allocation_count", 0)
        set("//sys/pool_trees/default/@config/fair_share_starvation_timeout", 1000)
        create_pool("pool_without_guarantee")
        create_pool("pool_with_guarantee", attributes={"strong_guarantee_resources": {"gpu": 1}})

        op1 = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out1",
            command="echo AAA >&2",
            spec={
                "max_failed_job_count": 1,
                "pool": "pool_without_guarantee",
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/base_layer"],
                    "enable_gpu_layers": True,
                    "gpu_check_layer_name": "0",
                    "gpu_check_binary_path": "/gpu_check/gpu_check_sleep",
                },
            },
        )

        wait(lambda: op1.list_jobs())
        job_id1 = op1.list_jobs()[0]

        wait(lambda: gpu_check_is_running(job_id1), timeout=INCREASED_TIMEOUT)

        op2 = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out2",
            command="echo AAA >&2",
            spec={
                "max_failed_job_count": 1,
                "pool": "pool_with_guarantee",
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/base_layer"],
                    "enable_gpu_layers": True,
                    "gpu_check_layer_name": "0",
                    "gpu_check_binary_path": "/gpu_check/gpu_check_sleep",
                },
            },
        )

        wait(lambda: op2.list_jobs())
        job_id2 = op2.list_jobs()[0]

        wait(lambda: len(op1.list_jobs()) == 0)
        wait(lambda: get_job(op2.id, job_id2)["state"] == "running")

    @authors("eshcherbin")
    @pytest.mark.timeout(180)
    def test_gpu_check_args(self):
        self.setup_gpu_layer_and_reset_nodes()

        op = run_test_vanilla(
            command="echo AAA >&2",
            spec={
                "max_failed_job_count": 1,
            },
            task_patch={
                "layer_paths": ["//tmp/base_layer"],
                "enable_gpu_layers": True,
                "gpu_check_layer_name": "0",
                "gpu_check_binary_path": "/gpu_check/gpu_check_args",
                "gpu_check_binary_args": ["-Y"],
            },
            track=True,
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        res = op.read_stderr(job_id)
        assert res == b"AAA\n"


@authors("ignat")
class TestExtraGpuCheckFailure(YTEnvSetup, GpuCheckBase):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_root_fs": True,
            },
            "gpu_manager": {
                "driver_version": "0",
                "testing": {
                    "test_resource": True,
                    "test_layers": True,
                    "test_gpu_count": 1,
                    "test_extra_gpu_check_command_failure": True,
                },
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "use_artifact_binds": True,
                    },
                },
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "gpu_check_layer_directory_path": "//tmp/gpu_check"
        }
    }

    @pytest.mark.timeout(180)
    def test_extra_gpu_check_failure(self):
        self.setup_gpu_layer_and_reset_nodes()

        self.setup_tables()

        node = ls("//sys/cluster_nodes")[0]

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command=with_breakpoint("BREAKPOINT; exit 146"),
            spec={
                "max_failed_job_count": 2,
                "fail_on_job_restart": True,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/base_layer"],
                    "enable_gpu_layers": True,
                    "gpu_check_layer_name": "0",
                    "gpu_check_binary_path": "/gpu_check/gpu_check_success",
                },
            },
            track=False,
        )

        wait_breakpoint(job_count=1)
        assert op.get_state() == "running"

        release_breakpoint()
        wait(lambda: op.get_state() == "failed", timeout=INCREASED_TIMEOUT)

        # In case of this error the job considered as aborted.
        assert op.get_job_count("aborted", from_orchid=False) == 1

        alerts_path = "//sys/cluster_nodes/{}/@alerts".format(node)
        wait(lambda: get(alerts_path))

        alerts = get(alerts_path)
        assert len(alerts) == 1
        assert "Testing extra GPU check command failed" in str(alerts[0])
