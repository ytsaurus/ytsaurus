from yt_env_setup import YTEnvSetup, wait, require_ytserver_root_privileges
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

    @require_ytserver_root_privileges
    def test_disabled_layer_locations(self):
        self.Env.kill_nodes()

        disabled_path = None
        for node in self.Env.configs["node"][:1]:
            for layer_location in node["data_node"]["volume_manager"]["layer_locations"]:
                try:
                    disabled_path = layer_location["path"]
                    os.mkdir(layer_location["path"])
                except OSError:
                    pass
                open(layer_location["path"] + "/disabled", "w")

        self.Env.start_nodes(sync=True)
        self.wait_for_nodes()

        self.Env.kill_nodes()
        os.unlink(disabled_path + "/disabled")
        self.Env.start_nodes(sync=True)
        self.wait_for_nodes()

        time.sleep(5)

    @require_ytserver_root_privileges
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

    @require_ytserver_root_privileges
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

        jobs_path = "//sys/operations/{0}/jobs".format(op.id)
        assert get(jobs_path + "/@count") == 1
        for job_id in ls(jobs_path):
            stderr_path = "{0}/{1}/stderr".format(jobs_path, job_id)
            assert "static-bin" in read_file(stderr_path)

    @require_ytserver_root_privileges
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

        jobs_path = "//sys/operations/{0}/jobs".format(op.id)
        assert get(jobs_path + "/@count") == 1
        for job_id in ls(jobs_path):
            stderr_path = "{0}/{1}/stderr".format(jobs_path, job_id)
            stderr = read_file(stderr_path)
            assert "static-bin" in stderr
            assert "test" in stderr

    @require_ytserver_root_privileges
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
