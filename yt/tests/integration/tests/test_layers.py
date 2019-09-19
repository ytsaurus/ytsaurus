from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE
from yt_commands import *

from yt.environment.porto_helpers import porto_avaliable

import pytest
import inspect
import os
import time


@pytest.mark.skip_if('not porto_avaliable()')
class TestLayers(YTEnvSetup):
    NUM_SCHEDULERS = 1
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "test_root_fs" : True,
            "slot_manager": {
                "job_environment" : {
                    "type" : "porto",
                },
            }
        }
    }

    REQUIRE_YTSERVER_ROOT_PRIVILIGES = True
    USE_PORTO_FOR_SERVERS = True

    def setup_files(self):
        current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

        create("file", "//tmp/layer1")
        file_name = os.path.join(current_dir, "layers/static-bin.tar.gz")
        write_file("//tmp/layer1", open(file_name).read())

        create("file", "//tmp/layer2")
        file_name = os.path.join(current_dir, "layers/test.tar.gz")
        write_file("//tmp/layer2", open(file_name).read())

        create("file", "//tmp/corrupted_layer")
        file_name = os.path.join(current_dir, "layers/corrupted.tar.gz")
        write_file("//tmp/corrupted_layer", open(file_name).read())

        create("file", "//tmp/static_cat")
        file_name = os.path.join(current_dir, "layers/static_cat")
        write_file("//tmp/static_cat", open(file_name).read())

        set("//tmp/static_cat/@executable", True)

    @authors("ilpauzner")
    def test_disabled_layer_locations(self):
        with Restarter(self.Env, NODES_SERVICE):
            disabled_path = None
            for node in self.Env.configs["node"][:1]:
                for layer_location in node["data_node"]["volume_manager"]["layer_locations"]:
                    try:
                        disabled_path = layer_location["path"]
                        os.mkdir(layer_location["path"])
                    except OSError:
                        pass
                    open(layer_location["path"] + "/disabled", "w")

        wait_for_nodes()

        with Restarter(self.Env, NODES_SERVICE):
            os.unlink(disabled_path + "/disabled")
        wait_for_nodes()

        time.sleep(5)

    @authors("prime")
    def test_corrupted_layer(self):
        self.setup_files()
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])
        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="./static_cat; ls $YT_ROOT_FS 1>&2",
                file="//tmp/static_cat",
                spec={
                    "max_failed_job_count" : 1,
                    "mapper" : {
                        "layer_paths" : ["//tmp/layer1", "//tmp/corrupted_layer"],
                    }
                })

    @authors("psushin")
    def test_one_layer(self):
        self.setup_files()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="./static_cat; ls $YT_ROOT_FS 1>&2",
            file="//tmp/static_cat",
            spec={
                "max_failed_job_count" : 1,
                "mapper" : {
                    "layer_paths" : ["//tmp/layer1"],
                }
            })

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        for job_id in ls(jobs_path):
            stderr_path = "{0}/{1}/stderr".format(jobs_path, job_id)
            assert "static-bin" in read_file(stderr_path)

    @authors("psushin")
    def test_two_layers(self):
        self.setup_files()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="./static_cat; ls $YT_ROOT_FS 1>&2",
            file="//tmp/static_cat",
            spec={
                "max_failed_job_count" : 1,
                "mapper" : {
                    "layer_paths" : ["//tmp/layer1", "//tmp/layer2"],
                }
            })

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        for job_id in ls(jobs_path):
            stderr_path = "{0}/{1}/stderr".format(jobs_path, job_id)
            stderr = read_file(stderr_path)
            assert "static-bin" in stderr
            assert "test" in stderr

    @authors("psushin")
    def test_bad_layer(self):
        self.setup_files()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])
        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="./static_cat; ls $YT_ROOT_FS 1>&2",
                file="//tmp/static_cat",
                spec={
                    "max_failed_job_count" : 1,
                    "mapper" : {
                        "layer_paths" : ["//tmp/layer1", "//tmp/bad_layer"],
                    }
                })

@pytest.mark.skip_if('not porto_avaliable()')
@authors("mrkastep")
class TestJobSetup(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "test_root_fs" : True,
            "job_controller": {
                "job_setup_command": {
                    "path": "/static-bin/static-bash",
                    "args": ["-c", "echo SETUP-OUTPUT > /setup_output_file"]
                }
            },
            "slot_manager": {
                "job_environment" : {
                    "type" : "porto",
                },
            }
        },
    }

    USE_PORTO_FOR_SERVERS = True
    REQUIRE_YTSERVER_ROOT_PRIVILIGES = True

    def setup_files(self):
        current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

        create("file", "//tmp/layer1", attributes={"replication_factor": 1})
        file_name = os.path.join(current_dir, "layers/static-bin.tar.gz")
        write_file("//tmp/layer1", open(file_name).read(), file_writer={"upload_replication_factor": 1})

    def test_setup_cat(self):
        self.setup_files()

        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command='$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/setup_output_file >&2',
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/layer1"],
                    "job_count": 1,
                    "enable_setup_commands": True,
                }
            })

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        job_id = ls(jobs_path)[0]

        res = op.read_stderr(job_id)
        assert res == "SETUP-OUTPUT\n"

@pytest.mark.skip_if('not porto_avaliable()')
@authors("mrkastep")
class TestGpuLayer(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1
    NUM_SECONDARY_MASTER_CELLS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "test_root_fs" : True,
            "job_controller": {
                "job_setup_command": {
                    "path": "/static-bin/static-bash",
                    "args": ["-c", "echo SETUP-OUTPUT > /setup_output_file"]
                },
                "test_gpu_resource": True,
                "test_gpu_layers": True,
                "gpu_manager": {
                    "driver_layer_directory_path": "//tmp/drivers",
                    "driver_version": "test_version",
                    "driver_layer_fetch_period": 10000,
                }
            },
            "slot_manager": {
                "job_environment" : {
                    "type" : "porto",
                },
            }
        },
    }

    USE_PORTO_FOR_SERVERS = True
    REQUIRE_YTSERVER_ROOT_PRIVILIGES = True

    def setup_files(self):
        tx = start_transaction()

        current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

        create("map_node", "//tmp/drivers", tx=tx)
        create("file", "//tmp/drivers/test_version", attributes={"replication_factor": 1}, tx=tx)

        file_name = os.path.join(current_dir, "layers/static-bin.tar.gz")
        write_file("//tmp/drivers/test_version", open(file_name).read(), file_writer={"upload_replication_factor": 1}, tx=tx)

        create("file", "//tmp/layer2", attributes={"replication_factor": 1}, tx=tx)
        file_name = os.path.join(current_dir, "layers/test.tar.gz")
        write_file("//tmp/layer2", open(file_name).read(), file_writer={"upload_replication_factor": 1}, tx=tx)

        commit_transaction(tx)

    def test_setup_cat_gpu_layer(self):
        self.setup_files()

        create("table", "//tmp/t_in", attributes={"replication_factor": 1}, file_writer={"upload_replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1}, file_writer={"upload_replication_factor": 1})

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command='$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/setup_output_file >&2',
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/layer2"],
                    "enable_setup_commands": True,
                    "enable_gpu_layers": True,
                }
            })

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        job_id = ls(jobs_path)[0]

        res = op.read_stderr(job_id)
        assert res == "SETUP-OUTPUT\n"


@pytest.mark.skip_if('not porto_avaliable()')
@authors("mrkastep")
class TestCudaLayer(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1
    NUM_SECONDARY_MASTER_CELLS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "test_root_fs" : True,
            "job_controller": {
                "job_setup_command": {
                    "path": "/static-bin/static-bash",
                    "args": ["-c", "echo SETUP-OUTPUT > /setup_output_file"]
                },
                "test_gpu_resource": True,
                "test_gpu_layers": True,
                "gpu_manager": {
                    "driver_version": "0",
                    "toolkit_min_driver_version": {
                        "0": "0"
                    }
                }
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            }
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "cuda_toolkit_layer_directory_path": "//tmp/cuda"
        }
    }

    USE_PORTO_FOR_SERVERS = True
    REQUIRE_YTSERVER_ROOT_PRIVILIGES = True

    def setup_files(self):
        current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

        create("map_node", "//tmp/cuda")
        create("file", "//tmp/cuda/0", attributes={"replication_factor": 1})

        file_name = os.path.join(current_dir, "layers/static-bin.tar.gz")
        write_file("//tmp/cuda/0", open(file_name).read(), file_writer={"upload_replication_factor": 1})

        create("file", "//tmp/layer2", attributes={"replication_factor": 1})
        file_name = os.path.join(current_dir, "layers/test.tar.gz")
        write_file("//tmp/layer2", open(file_name).read(), file_writer={"upload_replication_factor": 1})

    def test_setup_cat_gpu_layer(self):
        self.setup_files()

        create("table", "//tmp/t_in", attributes={"replication_factor": 1}, file_writer={"upload_replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1}, file_writer={"upload_replication_factor": 1})

        write_table("//tmp/t_in", [{"k": 0}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command='$YT_ROOT_FS/static-bin/static-cat $YT_ROOT_FS/setup_output_file >&2',
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "job_count": 1,
                    "layer_paths": ["//tmp/layer2"],
                    "enable_setup_commands": True,
                    "enable_gpu_layers": True,
                    "cuda_toolkit_version": "0",
                }
            })

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        job_id = ls(jobs_path)[0]

        res = op.read_stderr(job_id)
        assert res == "SETUP-OUTPUT\n"
