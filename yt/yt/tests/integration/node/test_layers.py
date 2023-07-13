from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE

from yt_commands import (
    authors, wait, create, ls, get, set, remove, link, exists,
    write_file, write_table, get_job, abort_job,
    raises_yt_error, read_table, run_test_vanilla, map, wait_for_nodes, update_nodes_dynamic_config)

from yt.common import YtError
import yt.yson as yson

from yt_helpers import profiler_factory

from pydantic.utils import deep_update

import pytest

import os
import time

from collections import Counter


class TestLayers(YTEnvSetup):
    NUM_SCHEDULERS = 1
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "test_root_fs": True,
            "use_artifact_binds": True,
            "use_common_root_fs_quota": True,
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        }
    }

    USE_PORTO = True

    def setup_files(self):
        create("file", "//tmp/layer1")
        write_file("//tmp/layer1", open("layers/static-bin.tar", "rb").read())

        create("file", "//tmp/layer1.gz")
        write_file("//tmp/layer1.gz", open("layers/static-bin.tar.gz", "rb").read())

        create("file", "//tmp/layer1.xz")
        write_file("//tmp/layer1.xz", open("layers/static-bin.tar.xz", "rb").read())

        create("file", "//tmp/layer1.zstd")
        write_file("//tmp/layer1.zstd", open("layers/static-bin.tar.zstd", "rb").read())

        create("file", "//tmp/layer2")
        write_file("//tmp/layer2", open("layers/test.tar.gz", "rb").read())

        create("file", "//tmp/corrupted_layer")
        write_file("//tmp/corrupted_layer", open("layers/corrupted.tar.gz", "rb").read())

        create("file", "//tmp/static_cat")
        write_file("//tmp/static_cat", open("layers/static_cat", "rb").read())

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
                    with open(layer_location["path"] + "/disabled", "w"):
                        pass

        wait_for_nodes()

        with Restarter(self.Env, NODES_SERVICE):
            os.unlink(disabled_path + "/disabled")
        wait_for_nodes()

        time.sleep(5)

    @authors("prime")
    @pytest.mark.timeout(150)
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
                    "max_failed_job_count": 1,
                    "mapper": {
                        "layer_paths": ["//tmp/layer1", "//tmp/corrupted_layer"],
                    },
                },
            )

        # YT-14186: Corrupted user layer should not disable jobs on node.
        for node in ls("//sys/cluster_nodes"):
            assert len(get("//sys/cluster_nodes/{}/@alerts".format(node))) == 0

    @authors("psushin")
    @pytest.mark.parametrize("layer_compression", ["", ".gz", ".xz"])
    def test_one_layer(self, layer_compression):
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
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/layer1" + layer_compression],
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        for job_id in job_ids:
            assert b"static-bin" in op.read_stderr(job_id)

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
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/layer1", "//tmp/layer2"],
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        for job_id in job_ids:
            stderr = op.read_stderr(job_id)
            assert b"static-bin" in stderr
            assert b"test" in stderr

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
                    "max_failed_job_count": 1,
                    "mapper": {
                        "layer_paths": ["//tmp/layer1", "//tmp/bad_layer"],
                    },
                },
            )

    @authors("prime")
    def test_squashfs_spec_options(self):
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
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/layer1"],
                },
                "enable_squashfs": True,
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        for job_id in job_ids:
            assert b"static-bin" in op.read_stderr(job_id)

    @authors("galtsev")
    def test_default_base_layer(self):
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
                "max_failed_job_count": 1,
                "default_base_layer_path": "//tmp/layer1",
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        for job_id in job_ids:
            assert b"static-bin" in op.read_stderr(job_id)


class TestProbingLayer(TestLayers):
    NUM_TEST_PARTITIONS = 5

    INPUT_TABLE = "//tmp/input_table"
    OUTPUT_TABLE = "//tmp/output_table"

    MAX_TRIES = 3

    @staticmethod
    def create_tables(job_count):
        create("table", TestProbingLayer.INPUT_TABLE)
        create("table", TestProbingLayer.OUTPUT_TABLE)

        for key in range(job_count):
            write_table(f"<append=%true>{TestProbingLayer.INPUT_TABLE}", [{"k": key, "layer": "LAYER"}])

    @staticmethod
    def get_spec(user_slots, **options):
        spec = {
            "default_base_layer_path": "//tmp/layer2",
            "job_experiment": {
                "job_experiment_type": "base_layer",
                "base_layer_path": "//tmp/layer1",
                "alert_on_any_experiment_failure": True,
            },
            "mapper": {
                "format": "json",
            },
            "data_weight_per_job": 1,
            "resource_limits": {
                "user_slots": user_slots,
            },
            "max_failed_job_count": 0,
        }
        return deep_update(spec, options)

    @staticmethod
    def run_map(command, job_count, user_slots, **options):
        op = map(
            in_=TestProbingLayer.INPUT_TABLE,
            out=TestProbingLayer.OUTPUT_TABLE,
            command=command,
            spec=TestProbingLayer.get_spec(user_slots, **options),
        )

        assert get(f"{TestProbingLayer.INPUT_TABLE}/@row_count") == get(f"{TestProbingLayer.OUTPUT_TABLE}/@row_count")

        assert op.get_job_count("completed") == job_count

        return op

    @authors("galtsev")
    def test_probing_layer_success(self):
        self.setup_files()

        job_count = 7
        self.create_tables(job_count)

        command = (
            "if test -e $YT_ROOT_FS/test; then "
            "    sed 's/LAYER/control/'; sleep 0.1; "
            "else "
            "    sed 's/LAYER/treatment/'; "
            "fi"
        )

        for try_count in range(self.MAX_TRIES + 1):
            op = self.run_map(command, job_count, user_slots=1)

            assert op.get_job_count("failed") == 0

            counter = Counter([row["layer"] for row in read_table(self.OUTPUT_TABLE)])

            if counter["control"] >= 1 and counter["treatment"] >= 2:
                break

        assert try_count < self.MAX_TRIES

    @authors("galtsev")
    def test_probing_layer_failure(self):
        self.setup_files()

        job_count = 7
        self.create_tables(job_count)

        command = (
            "if test -e $YT_ROOT_FS/test; then "
            "    sed 's/LAYER/control/g'; sleep 0.1; "
            "else "
            "    sed 's/LAYER/treatment/g'; exit 1; "
            "fi"
        )

        alert_count = 0

        for try_count in range(self.MAX_TRIES + 1):
            op = self.run_map(command, job_count, user_slots=1)

            assert op.get_job_count("failed") == 0

            counter = Counter([row["layer"] for row in read_table(self.OUTPUT_TABLE)])
            assert counter["control"] == job_count
            assert counter["treatment"] == 0

            assert op.get_job_count("aborted") == 2 or "base_layer_probe_failed" in op.get_alerts()

            if "base_layer_probe_failed" in op.get_alerts():
                alert_count += 1

            if op.get_job_count("aborted") >= 2:
                break

        assert try_count < self.MAX_TRIES

        assert alert_count >= 1

    @authors("galtsev")
    @pytest.mark.parametrize("options", [
        {"fail_on_job_restart": True},
        {"mapper": {"layer_paths": ["//tmp/layer2"]}},
        {"max_speculative_job_count_per_task": 0},
        {"try_avoid_duplicating_jobs": True},
    ])
    def test_probing_layer_disabled(self, options):
        self.setup_files()

        job_count = 7
        self.create_tables(job_count)

        command = (
            "if test -e $YT_ROOT_FS/test; then "
            "    sed 's/LAYER/control/g'; "
            "else "
            "    sed 's/LAYER/treatment/g'; "
            "fi"
        )

        op = self.run_map(command, job_count, user_slots=1, **options)

        assert op.get_job_count("failed") == 0

        counter = Counter([row["layer"] for row in read_table(self.OUTPUT_TABLE)])
        assert counter["control"] == job_count
        assert counter["treatment"] == 0

        assert op.get_job_count("aborted") == 0

    @authors("galtsev")
    @pytest.mark.timeout(600)
    def test_probing_layer_races(self):
        self.setup_files()

        job_count = 10
        self.create_tables(job_count)

        command = (
            "if test -e $YT_ROOT_FS/test; then "
            "    sed 's/LAYER/control/'; "
            "else "
            "    sed 's/LAYER/treatment/'; "
            "fi"
        )

        for iterations in range(3):
            for try_count in range(self.MAX_TRIES + 1):
                op = self.run_map(command, job_count, user_slots=2 + iterations)

                assert op.get_job_count("failed") == 0

                if op.get_job_count("aborted") >= 1:
                    break

            assert try_count < self.MAX_TRIES

    @authors("galtsev")
    @pytest.mark.timeout(600)
    def test_probing_layer_alert(self):
        self.setup_files()

        job_count = 10
        self.create_tables(job_count)
        alert_count = 0

        for control_failure_rate in range(2, 5):
            for treatment_failure_rate in range(2, 5):

                command = (
                    f"if test -e $YT_ROOT_FS/test; then "
                    f"    if [ $(($RANDOM % {control_failure_rate})) -eq 0 ]; then "
                    f"        exit 1; "
                    f"    fi; "
                    f"    sed 's/LAYER/control/g'; "
                    f"else "
                    f"    if [ $(($RANDOM % {treatment_failure_rate})) -eq 0 ]; then "
                    f"        exit 1; "
                    f"    fi; "
                    f"    sed 's/LAYER/treatment/g'; "
                    f"fi"
                )

                op = self.run_map(command, job_count, user_slots=5, max_failed_job_count=1000)

                counter = Counter([row["layer"] for row in read_table(self.OUTPUT_TABLE)])

                if "base_layer_probe_failed" in op.get_alerts():
                    attributes = op.get_alerts()["base_layer_probe_failed"]["attributes"]
                    assert attributes["failed_control_job_count"] == op.get_job_count("failed")
                    assert attributes["succeeded_treatment_job_count"] > 0 or counter["treatment"] == 0
                    alert_count += 1

        assert alert_count > 1


class TestDockerImage(TestLayers):
    INPUT_TABLE = "//tmp/input_table"
    OUTPUT_TABLE = "//tmp/output_table"
    COMMAND = "test -e $YT_ROOT_FS/test && test -e $YT_ROOT_FS/static-bin"
    IMAGE = "tmp/test-image"
    TAG_DOCUMENT_PATH = f"//{IMAGE}/_tags"

    @staticmethod
    def create_tables():
        create("table", TestDockerImage.INPUT_TABLE)
        create("table", TestDockerImage.OUTPUT_TABLE)

        write_table(TestDockerImage.INPUT_TABLE, [{"a": 1}])

    @staticmethod
    def create_mock_docker_image(document):
        create(
            "document",
            TestDockerImage.TAG_DOCUMENT_PATH,
            attributes={"value": document},
            recursive=True,
        )

    @staticmethod
    def run_map(docker_image, **kwargs):
        spec = {
            "mapper": {
                "docker_image": docker_image,
            },
        }
        spec["mapper"].update(kwargs)

        map(
            in_=TestDockerImage.INPUT_TABLE,
            out=TestDockerImage.OUTPUT_TABLE,
            command=TestDockerImage.COMMAND,
            spec=spec,
        )

    @authors("galtsev")
    def test_docker_image_success(self):
        self.setup_files()
        self.create_tables()

        tag = "tag"
        self.create_mock_docker_image({tag: ["//tmp/layer1", "//tmp/layer2"]})

        self.run_map(f"{TestDockerImage.IMAGE}:{tag}")

    @authors("galtsev")
    def test_docker_image_and_layer_paths(self):
        self.setup_files()
        self.create_tables()

        tag = "tag"
        self.create_mock_docker_image({tag: ["//tmp/layer1"]})

        self.run_map(f"{TestDockerImage.IMAGE}:{tag}", layer_paths=["//tmp/layer2"])

    @authors("galtsev")
    def test_default_docker_tag(self):
        self.setup_files()
        self.create_tables()

        default_docker_tag = "latest"
        self.create_mock_docker_image({default_docker_tag: ["//tmp/layer1", "//tmp/layer2"]})

        self.run_map(f"{TestDockerImage.IMAGE}")

    @authors("galtsev")
    def test_no_tag(self):
        self.setup_files()
        self.create_tables()

        tag = "tag"
        wrong_tag = "wrong_tag"
        self.create_mock_docker_image({tag: ["//tmp/layer1", "//tmp/layer2"]})

        with raises_yt_error(f'No tag "{wrong_tag}" in "{TestDockerImage.TAG_DOCUMENT_PATH}", available tags are [{tag}]'):
            self.run_map(f"{TestDockerImage.IMAGE}:{wrong_tag}")

    @authors("galtsev")
    def test_no_image(self):
        self.setup_files()
        self.create_tables()

        with raises_yt_error(f'Failed to read tags from "{TestDockerImage.TAG_DOCUMENT_PATH}"'):
            self.run_map(f"{TestDockerImage.IMAGE}:tag")

    @authors("galtsev")
    def test_wrong_tag_document_type(self):
        self.setup_files()
        self.create_tables()

        self.create_mock_docker_image("wrong tag document type")

        with raises_yt_error(f'Tags document "{TestDockerImage.TAG_DOCUMENT_PATH}" is not a map'):
            self.run_map(f"{TestDockerImage.IMAGE}:tag")


@authors("psushin")
class TestTmpfsLayerCache(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "test_root_fs": True,
            "use_artifact_binds": True,
            "use_common_root_fs_quota": True,
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
        "data_node": {
            "volume_manager": {
                "regular_tmpfs_layer_cache": {
                    "capacity": 10 * 1024 * 1024,
                    "layers_update_period": 100,
                },
                "nirvana_tmpfs_layer_cache": {
                    "capacity": 10 * 1024 * 1024,
                    "layers_update_period": 100,
                }
            }
        },
    }

    USE_PORTO = True

    def setup_files(self):
        create("file", "//tmp/layer1", attributes={"replication_factor": 1})
        file_name = "layers/static-bin.tar.gz"
        write_file("//tmp/layer1", open(file_name, "rb").read())

        create("file", "//tmp/static_cat", attributes={"replication_factor": 1})
        file_name = "layers/static_cat"
        write_file("//tmp/static_cat", open(file_name, "rb").read())

        set("//tmp/static_cat/@executable", True)

    def test_tmpfs_layer_cache(self):
        self.setup_files()

        orchid_path = "orchid/job_controller/slot_manager/root_volume_manager"

        for node in ls("//sys/cluster_nodes"):
            assert get("//sys/cluster_nodes/{0}/{1}/regular_tmpfs_cache/layer_count".format(node, orchid_path)) == 0
            assert get("//sys/cluster_nodes/{0}/{1}/nirvana_tmpfs_cache/layer_count".format(node, orchid_path)) == 0

        create("map_node", "//tmp/cached_layers")
        link("//tmp/layer1", "//tmp/cached_layers/layer1")

        with Restarter(self.Env, NODES_SERVICE):
            # First we create cypress map node for cached layers,
            # and then add it to node config with node restart.
            # Otherwise environment starter will consider node as dead, since
            # it will not be able to initialize tmpfs layer cache and will
            # report zero user job slots.
            for i, config in enumerate(self.Env.configs["node"]):
                config["data_node"]["volume_manager"]["regular_tmpfs_layer_cache"]["layers_directory_path"] = "//tmp/cached_layers"
                config["data_node"]["volume_manager"]["nirvana_tmpfs_layer_cache"]["layers_directory_path"] = "//tmp/cached_layers"
                config_path = self.Env.config_paths["node"][i]
                with open(config_path, "wb") as fout:
                    yson.dump(config, fout)

        wait_for_nodes()
        for node in ls("//sys/cluster_nodes"):
            # After node restart we must wait for async root volume manager initialization.
            wait(lambda: exists("//sys/cluster_nodes/{0}/{1}".format(node, orchid_path)))
            wait(lambda: get("//sys/cluster_nodes/{0}/{1}/regular_tmpfs_cache/layer_count".format(node, orchid_path)) == 1)
            wait(lambda: get("//sys/cluster_nodes/{0}/{1}/nirvana_tmpfs_cache/layer_count".format(node, orchid_path)) == 1)

        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="./static_cat; ls $YT_ROOT_FS 1>&2",
            file="//tmp/static_cat",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/layer1"],
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]
        assert b"static-bin" in op.read_stderr(job_id)

        job = get_job(op.id, job_id)
        regular_cache_hits = profiler_factory().at_node(job["address"]).get("exec_node/layer_cache/tmpfs_cache_hits", {"cache_name": "regular"})
        nirvana_cache_hits = profiler_factory().at_node(job["address"]).get("exec_node/layer_cache/tmpfs_cache_hits", {"cache_name": "nirvana"})

        assert regular_cache_hits > 0 or nirvana_cache_hits > 0

        remove("//tmp/cached_layers/layer1")
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get("//sys/cluster_nodes/{0}/{1}/regular_tmpfs_cache/layer_count".format(node, orchid_path)) == 0)
            wait(lambda: get("//sys/cluster_nodes/{0}/{1}/nirvana_tmpfs_cache/layer_count".format(node, orchid_path)) == 0)


@authors("ignat")
class TestJobSetup(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "test_root_fs": True,
            "use_artifact_binds": True,
            "use_common_root_fs_quota": True,
            "job_controller": {
                "job_setup_command": {
                    "path": "/static-bin/static-bash",
                    "args": ["-c", "echo SETUP-OUTPUT > /setup_output_file"],
                }
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
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


@authors("prime")
class TestSquashfsLayers(TestLayers):
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "use_artifact_binds": True,
            "test_root_fs": True,
            "use_common_root_fs_quota": True,
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },

        "data_node": {
            "volume_manager": {
                "convert_layers_to_squashfs": True,
                "use_bundled_tar2squash": True,
            }
        },
    }


@authors("prime")
class TestSquashfsTmpfsLayerCache(TestTmpfsLayerCache):
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "test_root_fs": True,
            "use_artifact_binds": True,
            "use_common_root_fs_quota": True,
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },

        "data_node": {
            "volume_manager": {
                "convert_layers_to_squashfs": True,
                "use_bundled_tar2squash": True,

                "regular_tmpfs_layer_cache": {
                    "capacity": 10 * 1024 * 1024,
                    "layers_update_period": 100,
                },
                "nirvana_tmpfs_layer_cache": {
                    "capacity": 10 * 1024 * 1024,
                    "layers_update_period": 100,
                }
            }
        },
    }


@authors("eshcherbin")
class TestJobAbortDuringVolumePreparation(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "test_root_fs": True,
            "use_artifact_binds": True,
            "use_common_root_fs_quota": True,
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
            "job_abortion_timeout": 5000,
        },
    }

    USE_PORTO = True

    def setup_files(self):
        create("file", "//tmp/layer", attributes={"replication_factor": 1})
        file_name = "layers/test.tar.gz"
        write_file(
            "//tmp/layer",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

    def test_job_abort_during_volume_preparation(self):
        self.setup_files()

        update_nodes_dynamic_config({
            "exec_agent": {
                "volume_manager": {
                    "delay_after_layer_imported": 60000,
                },
            },
        })

        op = run_test_vanilla(
            command="sleep 1",
            task_patch={"layer_paths": ["//tmp/layer"]},
        )

        wait(lambda: op.list_jobs())

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        abort_job(job_id)
        wait(lambda: op.get_job_count("aborted") > 0)

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        for alert in get("//sys/cluster_nodes/{}/@alerts".format(node)):
            assert "Scheduler jobs disabled" not in alert["message"]
