from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE

from yt_commands import (
    authors, wait, create, ls, get, set, remove, link, exists,
    write_file, write_table, get_job, abort_job, poll_job_shell,
    raises_yt_error, read_table, run_test_vanilla, map, map_reduce,
    sort, wait_for_nodes, update_nodes_dynamic_config)

from yt.common import YtError, YtResponseError, update
import yt.yson as yson

from yt_helpers import profiler_factory

import pytest

import os
import re
import sys
import time
import tempfile

from builtins import set as Set
from collections import Counter


class TestLayersBase(YTEnvSetup):
    NUM_SCHEDULERS = 1

    INVALID_EXTERNAL_IMAGE = "registry.invalid/image:tag"

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


class TestLayers(TestLayersBase):
    USE_PORTO = True

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
        }
    }

    @authors("ilpauzner")
    def test_disabled_layer_locations(self):
        for node in self.Env.configs["node"]:
            for layer_location in node["data_node"]["volume_manager"]["layer_locations"]:
                try:
                    disabled_path = layer_location["path"]
                    os.mkdir(layer_location["path"])
                except OSError:
                    pass
                with open(layer_location["path"] + "/disabled", "w"):
                    pass

        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait_for_nodes()

        nodes = ls("//sys/cluster_nodes")

        def check_layer_cache_disable():
            for node in nodes:
                alerts = get("//sys/cluster_nodes/{}/@alerts".format(node))
                return any([alert["message"] == "Layer cache is disabled" for alert in alerts]) and \
                    any([alert["message"] == "Layer location disabled" for alert in alerts])

        wait(lambda: check_layer_cache_disable())

        for node in nodes:
            wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) == 0)

        for node in self.Env.configs["node"]:
            for layer_location in node["data_node"]["volume_manager"]["layer_locations"]:
                try:
                    disabled_path = layer_location["path"]
                    os.unlink(disabled_path + "/disabled")
                except OSError:
                    pass

        with Restarter(self.Env, NODES_SERVICE):
            pass

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

    @authors("galtsev")
    @pytest.mark.timeout(600)
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

    @authors("galtsev")
    @pytest.mark.timeout(600)
    def test_default_base_layer_with_layer_paths(self):
        self.setup_files()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0}], sorted_by="k")
        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            mapper_command="./static_cat; ls $YT_ROOT_FS 1>&2",
            reducer_command="if [ ! -e test ]; then exit 1; fi; cat",
            mapper_file=["//tmp/static_cat"],
            sort_by=["k"],
            spec={
                "max_failed_job_count": 1,
                "default_base_layer_path": "//tmp/layer1",
                "reducer": {
                    "layer_paths": ["//tmp/layer2"],
                },
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
                "base_layer_path": "//tmp/layer1",
                "alert_on_any_treatment_failure": True,
            },
            "mapper": {
                "format": "json",
            },
            "data_weight_per_job": 1,
            "resource_limits": {
                "user_slots": user_slots,
            },
        }
        return update(spec, options)

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

    @staticmethod
    def run_sort(user_slots, **options):
        op = sort(
            in_=TestProbingLayer.INPUT_TABLE,
            out=TestProbingLayer.OUTPUT_TABLE,
            sort_by="k",
            spec=TestProbingLayer.get_spec(user_slots, **options),
        )

        assert get(f"{TestProbingLayer.INPUT_TABLE}/@row_count") == get(f"{TestProbingLayer.OUTPUT_TABLE}/@row_count")

        return op

    @authors("galtsev")
    @pytest.mark.skip(reason="YT-21152, probing is tested in TestJobExperiment")
    @pytest.mark.flaky(max_runs=5)
    @pytest.mark.timeout(600)
    def test_probing_layer_success(self):
        self.setup_files()

        job_count = 20
        self.create_tables(job_count)

        command = (
            "if test -e $YT_ROOT_FS/test; then "
            "    sed 's/LAYER/control/'; sleep 0.1; "
            "else "
            "    sed 's/LAYER/treatment/'; "
            "fi"
        )

        max_tries = 10
        for try_count in range(max_tries + 1):
            op = self.run_map(command, job_count, user_slots=2)

            assert op.get_job_count("failed") == 0

            counter = Counter([row["layer"] for row in read_table(self.OUTPUT_TABLE)])

            if counter["control"] >= 1 and counter["treatment"] >= 2:
                break

        assert try_count < max_tries

    @authors("galtsev")
    @pytest.mark.skip(reason="YT-21152, probing is tested in TestJobExperiment")
    @pytest.mark.flaky(max_runs=5)
    @pytest.mark.timeout(600)
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
            op = self.run_map(command, job_count, user_slots=2)

            assert op.get_job_count("failed") == 0

            counter = Counter([row["layer"] for row in read_table(self.OUTPUT_TABLE)])
            assert counter["control"] == job_count
            assert counter["treatment"] == 0

            assert op.get_job_count("aborted") == 0 or "base_layer_probe_failed" in op.get_alerts()

            if "base_layer_probe_failed" in op.get_alerts():
                alert_count += 1

            if op.get_job_count("aborted") >= 2:
                break

        assert try_count < self.MAX_TRIES

        assert alert_count >= 1

    @authors("galtsev")
    @pytest.mark.skip(reason="YT-21152, probing is tested in TestJobExperiment")
    @pytest.mark.flaky(max_runs=5)
    @pytest.mark.parametrize("options", [
        {"fail_on_job_restart": True},
        {"mapper": {"layer_paths": ["//tmp/layer2"]}},
        {"max_speculative_job_count_per_task": 0},
        {"try_avoid_duplicating_jobs": True},
    ])
    @pytest.mark.timeout(600)
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
    @pytest.mark.skip(reason="YT-21152, probing is tested in TestJobExperiment")
    @pytest.mark.flaky(max_runs=5)
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
    @pytest.mark.skip(reason="YT-21152, probing is tested in TestJobExperiment")
    @pytest.mark.flaky(max_runs=5)
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

    @authors("galtsev")
    def test_probing_layer_crash(self):
        self.setup_files()

        job_count = 10
        self.create_tables(job_count)

        self.run_sort(user_slots=job_count)


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
            "max_failed_job_count": 1,
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
    @pytest.mark.timeout(180)
    def test_docker_image_success(self):
        self.setup_files()
        self.create_tables()

        tag = "tag"
        self.create_mock_docker_image({tag: ["//tmp/layer1", "//tmp/layer2"]})

        self.run_map(f"{TestDockerImage.IMAGE}:{tag}")

    @authors("galtsev")
    @pytest.mark.timeout(180)
    def test_docker_image_and_layer_paths(self):
        self.setup_files()
        self.create_tables()

        tag = "tag"
        self.create_mock_docker_image({tag: ["//tmp/layer1"]})

        self.run_map(f"{TestDockerImage.IMAGE}:{tag}", layer_paths=["//tmp/layer2"])

    @authors("galtsev")
    @pytest.mark.timeout(180)
    def test_default_docker_tag(self):
        self.setup_files()
        self.create_tables()

        default_docker_tag = "latest"
        self.create_mock_docker_image({default_docker_tag: ["//tmp/layer1", "//tmp/layer2"]})

        self.run_map(f"{TestDockerImage.IMAGE}")

    @authors("galtsev")
    @pytest.mark.timeout(180)
    def test_no_tag(self):
        self.setup_files()
        self.create_tables()

        tag = "tag"
        wrong_tag = "wrong_tag"
        self.create_mock_docker_image({tag: ["//tmp/layer1", "//tmp/layer2"]})

        with raises_yt_error(f'No tag "{wrong_tag}" in "{TestDockerImage.TAG_DOCUMENT_PATH}", available tags are [{tag}]'):
            self.run_map(f"{TestDockerImage.IMAGE}:{wrong_tag}")

    @authors("galtsev")
    @pytest.mark.timeout(180)
    def test_no_image(self):
        self.setup_files()
        self.create_tables()

        with raises_yt_error(f'Failed to read tags from "{TestDockerImage.TAG_DOCUMENT_PATH}"'):
            self.run_map(f"{TestDockerImage.IMAGE}:tag")

    @authors("galtsev")
    @pytest.mark.timeout(180)
    def test_wrong_tag_document_type(self):
        self.setup_files()
        self.create_tables()

        self.create_mock_docker_image("wrong tag document type")

        with raises_yt_error(f'Tags document "{TestDockerImage.TAG_DOCUMENT_PATH}" is not a map'):
            self.run_map(f"{TestDockerImage.IMAGE}:tag")

    @authors("khlebnikov")
    @pytest.mark.timeout(180)
    def test_invalid_external_image(self):
        self.create_tables()

        with raises_yt_error('External docker image is not supported in Porto job environment'):
            self.run_map(self.INVALID_EXTERNAL_IMAGE)


@authors("khlebnikov")
class TestCriDockerImage(TestLayersBase):
    JOB_ENVIRONMENT_TYPE = "cri"

    INPUT_TABLE = "//tmp/input_table"
    OUTPUT_TABLE = "//tmp/output_table"
    MAP_COMMAND = "cat"

    def create_tables(self):
        create("table", self.INPUT_TABLE)
        create("table", self.OUTPUT_TABLE)

        write_table(self.INPUT_TABLE, [{"a": 1}])

    def run_map(self, **kwargs):
        return map(
            in_=self.INPUT_TABLE,
            out=self.OUTPUT_TABLE,
            command=self.MAP_COMMAND,
            spec={
                "max_failed_job_count": 1,
                "mapper": kwargs,
            },
        )

    @authors("khlebnikov")
    @pytest.mark.timeout(180)
    def test_invalid_external_image(self):
        self.create_tables()

        with raises_yt_error('Failed to pull docker image'):
            self.run_map(docker_image=self.INVALID_EXTERNAL_IMAGE)

    @authors("khlebnikov")
    @pytest.mark.timeout(180)
    def test_unsupported_layers(self):
        self.create_tables()

        create("file", "//tmp/empty_layer")
        write_file("//tmp/empty_layer", b'\0'*1024)  # valid empty tar archive

        with raises_yt_error('Porto layers are not supported in CRI job environment'):
            self.run_map(layer_paths=["//tmp/empty_layer"])

    def _poll_until_shell_exited(self, job_id, shell_id):
        output = ""
        try:
            while True:
                rsp = poll_job_shell(job_id, operation="poll", shell_id=shell_id)
                output += rsp["output"]
        except YtResponseError as err:
            if err.is_shell_exited():
                return output
            raise

    @authors("khlebnikov")
    @pytest.mark.timeout(180)
    def test_environment_variables(self):
        docker_image = self.Env.yt_config.default_docker_image
        self.create_tables()
        map(
            in_=self.INPUT_TABLE,
            out=self.OUTPUT_TABLE,
            spec={
                "mapper": {
                    "docker_image": docker_image,
                    "command": 'printenv | sed -E \'s#"#\\\\"#g;s#([^=]*)=(.*)#{name="\\1"; value="\\2";};#\'',
                    "environment": {
                        "MY_VARIABLE_A": "MY_VALUE_A",
                        "MY_VARIABLE_B": "MY_VALUE_B",
                    },
                },
                "secure_vault": {
                    "MY_SECRET_A": "SECRET_VALUE_A",
                    "MY_SECRET_B": "SECRET_VALUE_B",
                },
                "max_failed_job_count": 1,
            },
        )
        output = read_table(self.OUTPUT_TABLE)
        env = {row["name"]: row["value"] for row in output}
        assert len(output) == len(env)

        guid_regex = r'^[0-9a-fA-F]{1,8}-[0-9a-fA-F]{1,8}-[0-9a-fA-F]{1,8}-[0-9a-fA-F]{1,8}$'
        is_guid = lambda x: re.match(guid_regex, x) is not None  # noqa
        expected = {
            # set by job environment
            "USER": lambda x: re.match(r'^[a-z][-a-z0-9]*$', x) is not None,
            "LOGNAME": lambda x: x == env["USER"],
            # set by ytserver-exec
            "SHELL": lambda x: x == "/bin/bash",
            # set by docker image
            "PATH": lambda x: Set(x.split(':')).issuperset({"/bin", "/usr/bin", "/usr/local/bin"}),
            # set by default docker image
            "PYTHON_VERSION": lambda x: x,
            # set by bash
            "PWD": lambda x: x == env["HOME"],
            # set by controller agent to $(SandboxPath) and expanded by job proxy
            "HOME": lambda x: x[0] == "/",
            "TMPDIR": lambda x: x == env["HOME"],
            # set by controller agent
            "YT_OPERATION_ID": is_guid,
            "YT_JOB_ID": is_guid,
            "YT_JOB_INDEX": lambda x: x == "0",
            "YT_TASK_JOB_INDEX": lambda x: x == "0",
            "YT_JOB_COOKIE": lambda x: x == "0",
            "YT_JOB_DOCKER_IMAGE": lambda x: x == docker_image,
            # set by operation spec
            "MY_VARIABLE_A": lambda x: x == "MY_VALUE_A",
            "MY_VARIABLE_B": lambda x: x == "MY_VALUE_B",
            "YT_SECURE_VAULT_MY_SECRET_A": lambda x: x == "SECRET_VALUE_A",
            "YT_SECURE_VAULT_MY_SECRET_B": lambda x: x == "SECRET_VALUE_B",
            "YT_SECURE_VAULT": lambda x: x == '{"MY_SECRET_A"="SECRET_VALUE_A";"MY_SECRET_B"="SECRET_VALUE_B";}',
        }
        for name, test in expected.items():
            assert name in env
            assert test(env[name]), "Failed test for {}={}".format(name, env[name])

        op = run_test_vanilla(
            "sleep 100",
            task_patch={
                "docker_image": docker_image,
                "environment": {
                    "MY_VARIABLE_A": "MY_VALUE_A",
                    "MY_VARIABLE_B": "MY_VALUE_B",
                },
            },
            spec={
                "secure_vault": {
                    "MY_SECRET_A": "SECRET_VALUE_A",
                    "MY_SECRET_B": "SECRET_VALUE_B",
                },
            },
        )
        wait(lambda: op.list_jobs())

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        # TODO(faucct): fix it after https://github.com/ytsaurus/ytsaurus/issues/1042
        time.sleep(1)
        env = self._poll_until_shell_exited(
            job_id, poll_job_shell(job_id, operation="spawn", command="env")["shell_id"]
        )
        env = {
            key: value
            for line in env.splitlines()
            for key, sep, value in [line.partition("=")]
            if sep
        }
        expected["TMPDIR"] = lambda x: x == env["HOME"] + "/tmp"
        for name, test in expected.items():
            assert name in env
            assert test(env[name]), "Failed test for {}={}".format(name, env[name])

        abort_job(job_id)


@authors("psushin")
class TestTmpfsLayerCache(YTEnvSetup):
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

        orchid_path = "orchid/exec_node/slot_manager/root_volume_manager"

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

        regular_tmpfs_layer_hits = profiler_factory().at_node(job["address"]).get("exec_node/layer_cache/tmpfs_layer_hits", {"cache_name": "regular", "cypress_path": "//tmp/layer1"})
        nirvanta_tmpfs_layer_hits = profiler_factory().at_node(job["address"]).get("exec_node/layer_cache/tmpfs_layer_hits", {"cache_name": "nirvana", "cypress_path": "//tmp/layer1"})
        assert regular_tmpfs_layer_hits > 0 or nirvanta_tmpfs_layer_hits > 0

        remove("//tmp/cached_layers/layer1")
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get("//sys/cluster_nodes/{0}/{1}/regular_tmpfs_cache/layer_count".format(node, orchid_path)) == 0)
            wait(lambda: get("//sys/cluster_nodes/{0}/{1}/nirvana_tmpfs_cache/layer_count".format(node, orchid_path)) == 0)


@authors("yuryalekseev")
class TestTmpfsLayers(YTEnvSetup):
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
        create("file", "//tmp/upper_layer", attributes={"replication_factor": 1})
        file_name = "layers/upper.tgz"
        write_file("//tmp/upper_layer", open(file_name, "rb").read())

        create("file", "//tmp/lower_layer", attributes={"replication_factor": 1})
        file_name = "layers/lower.tgz"
        write_file("//tmp/lower_layer", open(file_name, "rb").read())

        create("file", "//tmp/static_cat", attributes={"replication_factor": 1})
        file_name = "layers/static_cat"
        write_file("//tmp/static_cat", open(file_name, "rb").read())

        set("//tmp/static_cat/@executable", True)

    def test_trusted_overlay_opaque_extended_attributes(self):
        self.setup_files()

        orchid_path = "orchid/exec_node/slot_manager/root_volume_manager"

        for node in ls("//sys/cluster_nodes"):
            assert get("//sys/cluster_nodes/{0}/{1}/regular_tmpfs_cache/layer_count".format(node, orchid_path)) == 0
            assert get("//sys/cluster_nodes/{0}/{1}/nirvana_tmpfs_cache/layer_count".format(node, orchid_path)) == 0

        create("map_node", "//tmp/cached_layers")
        link("//tmp/upper_layer", "//tmp/cached_layers/upper_layer")

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
            command="./static_cat; ls $YT_ROOT_FS/dir 1>&2",
            file="//tmp/static_cat",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/upper_layer", "//tmp/lower_layer"],
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]
        job_stderr = op.read_stderr(job_id)
        # The trusted.overlay.opaque xattr is set on dir so ls should see upper file
        # from the upper layer and should not see lower file from the lower layer.
        assert b"upper" in job_stderr
        assert b"lower" not in job_stderr


@authors("ignat")
class TestJobSetup(YTEnvSetup):
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
                        "job_setup_command": {
                            "path": "/static-bin/static-bash",
                            "args": ["-c", "echo SETUP-OUTPUT > /setup_output_file"],
                        }
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

    def test_setup_cat(self):
        self.setup_files()

        config = get("//sys/cluster_nodes/@config")

        print(f"Dynamic config is {config}", file=sys.stderr)

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


@authors("eshcherbin")
class TestJobAbortDuringVolumePreparation(YTEnvSetup):
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
                        "waiting_for_job_cleanup_timeout": 5000,
                    },
                },
            },
        }
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
            "exec_node": {
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


@authors("yuryalekseev")
class TestLocalSquashFSLayers(YTEnvSetup):
    NUM_SCHEDULERS = 1
    DELTA_NODE_CONFIG = {
        "exec_node": {
            # This test_root_fs is for compatibility with 23.2 for now.
            "test_root_fs": True,
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
            "job_proxy": {
                "test_root_fs": True,
            },
        }
    }

    USE_PORTO = True

    def setup_files(self):
        create("file", "//tmp/corrupted_layer")
        write_file("//tmp/corrupted_layer", open("layers/corrupted.tar.gz", "rb").read())

        create("file", "//tmp/corrupted_squashfs.img")
        write_file("//tmp/corrupted_squashfs.img", open("layers/corrupted.tar.gz", "rb").read())
        set("//tmp/corrupted_squashfs.img/@access_method", "local")
        set("//tmp/corrupted_squashfs.img/@filesystem", "squashfs")

        create("file", "//tmp/squashfs.img")
        write_file("//tmp/squashfs.img", open("layers/squashfs.img", "rb").read())
        set("//tmp/squashfs.img/@access_method", "local")
        set("//tmp/squashfs.img/@filesystem", "squashfs")

        create("file", "//tmp/empty_squashfs.img")
        set("//tmp/squashfs.img/@access_method", "local")
        set("//tmp/squashfs.img/@filesystem", "squashfs")

    @authors("yuryalekseev")
    def test_empty_squashfs_layer(self):
        self.setup_files()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])

        with pytest.raises(YtError) as err:
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="ls $YT_ROOT_FS/dir 1>&2",
                spec={
                    "max_failed_job_count": 1,
                    "mapper": {
                        "layer_paths": ["//tmp/empty_squashfs.img"],
                    },
                },
            )

        assert "empty" in str(err)

        # YT-14186: Empty user layer should not disable jobs on node.
        for node in ls("//sys/cluster_nodes"):
            assert len(get("//sys/cluster_nodes/{}/@alerts".format(node))) == 0

    @authors("yuryalekseev")
    def test_squashfs_layer(self):
        self.setup_files()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])

        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="ls $YT_ROOT_FS/dir 1>&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/squashfs.img"],
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        for job_id in job_ids:
            assert b"squash_file" in op.read_stderr(job_id)

        # Check solomon counters.
        job = get_job(op.id, job_id)
        profiler = profiler_factory().at_node(job["address"])
        tags = {'type': 'squashfs', 'file_path': '//tmp/squashfs.img'}

        wait(lambda: profiler.get("volumes/created", tags) is not None)
        wait(lambda: profiler.get("volumes/create_time", tags) is not None)

        wait(lambda: profiler.get("volumes/removed", tags) is not None)
        wait(lambda: profiler.get("volumes/remove_time", tags) is not None)

    @authors("yuryalekseev")
    @pytest.mark.timeout(150)
    def test_corrupted_squashfs_layer(self):
        self.setup_files()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])
        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="ls $YT_ROOT_FS 1>&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/corrupted_squashfs.img"],
                },
            },
        )

        with pytest.raises(YtError):
            op.track()

        # YT-14186: Corrupted user layer should not disable jobs on node.
        for node in ls("//sys/cluster_nodes"):
            assert len(get("//sys/cluster_nodes/{}/@alerts".format(node))) == 0

        # Check solomon counters.
        job_ids = op.list_jobs()
        assert len(job_ids) == 1

        job = get_job(op.id, job_ids[0])
        profiler = profiler_factory().at_node(job["address"])
        tags = {'type': 'squashfs', 'file_path': '//tmp/corrupted_squashfs.img'}
        wait(lambda: profiler.get("volumes/created", tags) is not None)
        wait(lambda: profiler.get("volumes/create_errors", tags) is not None)

    @authors("yuryalekseev")
    @pytest.mark.timeout(150)
    def test_corrupted_layer_with_squashfs_layer(self):
        self.setup_files()
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])
        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="ls $YT_ROOT_FS 1>&2",
                spec={
                    "max_failed_job_count": 1,
                    "mapper": {
                        "layer_paths": ["//tmp/squashfs.img", "//tmp/corrupted_layer"],
                    },
                },
            )

        # YT-14186: Corrupted user layer should not disable jobs on node.
        for node in ls("//sys/cluster_nodes"):
            assert len(get("//sys/cluster_nodes/{}/@alerts".format(node))) == 0


@authors("yuryalekseev")
class TestNbdSquashFSLayers(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

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
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "nbd": {
                    "block_cache_compressed_data_capacity": 536870912,
                    "client": {
                        "io_timeout": 30000,
                    },
                    "enabled": True,
                    "server": {
                        "unix_domain_socket": {
                            # The best would be to use os.path.join(self.path_to_run, tempfile.mkstemp(dir="/tmp")[1]),
                            # but it leads to a path with length greater than the maximum allowed 108 bytes.
                            # So put it at home directory until PORTO-1242 is done, then put it in /tmp.
                            "path": tempfile.mkstemp(dir="/tmp" if "USER" not in os.environ else "/root" if os.environ["USER"] == "root" else "/home/" + os.environ["USER"])[1]
                        },
                    },
                },
            },
        }
    }

    USE_PORTO = True

    def setup_files(self):
        create("file", "//tmp/corrupted_layer")
        write_file("//tmp/corrupted_layer", open("layers/corrupted.tar.gz", "rb").read())

        create("file", "//tmp/corrupted_squashfs.img")
        write_file("//tmp/corrupted_squashfs.img", open("layers/corrupted.tar.gz", "rb").read())
        set("//tmp/corrupted_squashfs.img/@access_method", "nbd")
        set("//tmp/corrupted_squashfs.img/@filesystem", "squashfs")

        create("file", "//tmp/squashfs.img")
        write_file("//tmp/squashfs.img", open("layers/squashfs.img", "rb").read())
        set("//tmp/squashfs.img/@access_method", "nbd")
        set("//tmp/squashfs.img/@filesystem", "squashfs")

    @authors("yuryalekseev")
    def test_squashfs_layer(self):
        self.setup_files()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])

        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="ls $YT_ROOT_FS/dir 1>&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/squashfs.img"],
                },
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        for job_id in job_ids:
            assert b"squash_file" in op.read_stderr(job_id)

        # Check solomon counters.
        job = get_job(op.id, job_id)
        profiler = profiler_factory().at_node(job["address"])
        tags = {'file_path': '//tmp/squashfs.img'}

        wait(lambda: profiler.get("nbd/server/count") is not None)
        wait(lambda: profiler.get("nbd/server/created") is not None)
        wait(lambda: profiler.get("nbd/device/count", tags) is not None)

        wait(lambda: profiler.get("nbd/device/created", tags) is not None)
        wait(lambda: profiler.get("nbd/device/removed", tags) is not None)

        wait(lambda: profiler.get("nbd/device/registered", tags) is not None)
        wait(lambda: profiler.get("nbd/device/unregistered", tags) is not None)

        wait(lambda: profiler.get("nbd/device/read_count", tags) is not None)
        wait(lambda: profiler.get("nbd/device/read_bytes", tags) is not None)
        wait(lambda: profiler.get("nbd/device/read_time", tags) is not None)

        wait(lambda: profiler.get("nbd/device/read_block_bytes_from_cache", tags) is not None)
        wait(lambda: profiler.get("nbd/device/read_block_bytes_from_disk", tags) is not None)

        tags = {'type': 'nbd', 'file_path': '//tmp/squashfs.img'}

        wait(lambda: profiler.get("volumes/created", tags) is not None)
        wait(lambda: profiler.get("volumes/create_time", tags) is not None)

        wait(lambda: profiler.get("volumes/removed", tags) is not None)
        wait(lambda: profiler.get("volumes/remove_time", tags) is not None)

    @authors("yuryalekseev")
    @pytest.mark.timeout(150)
    def test_corrupted_squashfs_layer(self):
        self.setup_files()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])
        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="ls $YT_ROOT_FS 1>&2",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/corrupted_squashfs.img"],
                },
            },
        )

        with pytest.raises(YtError):
            op.track()

        # YT-14186: Corrupted user layer should not disable jobs on node.
        for node in ls("//sys/cluster_nodes"):
            assert len(get("//sys/cluster_nodes/{}/@alerts".format(node))) == 0

        # Check solomon counters.
        job_ids = op.list_jobs()
        assert len(job_ids) == 1

        job = get_job(op.id, job_ids[0])
        profiler = profiler_factory().at_node(job["address"])
        tags = {'type': 'nbd', 'file_path': '//tmp/corrupted_squashfs.img'}
        wait(lambda: profiler.get("volumes/created", tags) is not None)
        wait(lambda: profiler.get("volumes/create_errors", tags) is not None)

        tags = {'file_path': '//tmp/corrupted_squashfs.img'}
        wait(lambda: profiler.get("nbd/device/created", tags) is not None)
        wait(lambda: profiler.get("nbd/device/removed", tags) is not None)

    @authors("yuryalekseev")
    @pytest.mark.timeout(150)
    def test_corrupted_layer_with_squashfs_layer(self):
        self.setup_files()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])
        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="ls $YT_ROOT_FS 1>&2",
                spec={
                    "max_failed_job_count": 1,
                    "mapper": {
                        "layer_paths": ["//tmp/squashfs.img", "//tmp/corrupted_layer"],
                    },
                },
            )

        # YT-14186: Corrupted user layer should not disable jobs on node.
        for node in ls("//sys/cluster_nodes"):
            assert len(get("//sys/cluster_nodes/{}/@alerts".format(node))) == 0


@authors("yuryalekseev")
class TestNbdConnectionFailuresWithSquashFSLayers(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

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
        }
    }

    USE_PORTO = True

    def setup_files(self):
        create("file", "//tmp/squashfs.img")
        write_file("//tmp/squashfs.img", open("layers/squashfs.img", "rb").read())
        set("//tmp/squashfs.img/@access_method", "nbd")
        set("//tmp/squashfs.img/@filesystem", "squashfs")

    @authors("yuryalekseev")
    def test_read_timeout(self):
        self.setup_files()

        update_nodes_dynamic_config({
            "exec_node": {
                "nbd": {
                    "block_cache_compressed_data_capacity": 536870912,
                    "client": {
                        # Set read I/O timeout to 1 second
                        "io_timeout": 1000,
                    },
                    "enabled": True,
                    "server": {
                        "unix_domain_socket": {
                            # The best would be to use os.path.join(self.path_to_run, tempfile.mkstemp(dir="/tmp")[1]),
                            # but it leads to a path with length greater than the maximum allowed 108 bytes.
                            # So put it at home directory until PORTO-1242 is done, then put it in /tmp.
                            "path": tempfile.mkstemp(dir="/root" if os.environ["USER"] == "root" else "/home/" + os.environ["USER"])[1]
                        },
                        # Sleep for 10 seconds prior to performing read I/O
                        "test_block_device_sleep_before_read": 10000,
                    },
                },
            },
        })

        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait_for_nodes()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="ls $YT_ROOT_FS/dir 1>&2",
                spec={
                    "max_failed_job_count": 1,
                    "mapper": {
                        "layer_paths": ["//tmp/squashfs.img"],
                    },
                },
            )

        # YT-14186: Corrupted user layer should not disable jobs on node.
        for node in ls("//sys/cluster_nodes"):
            assert len(get("//sys/cluster_nodes/{}/@alerts".format(node))) == 0

    @authors("yuryalekseev")
    def test_abort_on_read(self):
        self.setup_files()

        update_nodes_dynamic_config({
            "exec_node": {
                "nbd": {
                    "block_cache_compressed_data_capacity": 536870912,
                    "client": {
                        # Set read I/O timeout to 1 second
                        "io_timeout": 1000,
                    },
                    "enabled": True,
                    "server": {
                        "unix_domain_socket": {
                            # The best would be to use os.path.join(self.path_to_run, tempfile.mkstemp(dir="/tmp")[1]),
                            # but it leads to a path with length greater than the maximum allowed 108 bytes.
                            # So put it at home directory until PORTO-1242 is done, then put it in /tmp.
                            "path": tempfile.mkstemp(dir="/root" if os.environ["USER"] == "root" else "/home/" + os.environ["USER"])[1]
                        },
                        # Abort connection prior to read I/O
                        "test_abort_connection_on_read": True,
                    },
                },
            },
        })

        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait_for_nodes()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="ls $YT_ROOT_FS/dir 1>&2",
                spec={
                    "max_failed_job_count": 1,
                    "mapper": {
                        "layer_paths": ["//tmp/squashfs.img"],
                    },
                },
            )

        # YT-14186: Corrupted user layer should not disable jobs on node.
        for node in ls("//sys/cluster_nodes"):
            assert len(get("//sys/cluster_nodes/{}/@alerts".format(node))) == 0


@authors("yuryalekseev")
class TestInvalidAttributeValues(YTEnvSetup):
    NUM_SCHEDULERS = 1

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
        }
    }

    USE_PORTO = True

    def setup_files(self, access_method, filesystem):
        create("file", "//tmp/squashfs.img")
        write_file("//tmp/squashfs.img", open("layers/squashfs.img", "rb").read())
        set("//tmp/squashfs.img/@access_method", access_method)
        set("//tmp/squashfs.img/@filesystem", filesystem)

    @authors("yuryalekseev")
    def test_invalid_access_method(self):
        self.setup_files(access_method="invalid", filesystem="squashfs")

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="ls $YT_ROOT_FS/dir 1>&2",
                spec={
                    "max_failed_job_count": 1,
                    "mapper": {
                        "layer_paths": ["//tmp/squashfs.img"],
                    },
                },
            )

    @authors("yuryalekseev")
    def test_invalid_filesystem(self):
        self.setup_files(access_method="local", filesystem="invalid")

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="ls $YT_ROOT_FS/dir 1>&2",
                spec={
                    "max_failed_job_count": 1,
                    "mapper": {
                        "layer_paths": ["//tmp/squashfs.img"],
                    },
                },
            )


@authors("yuryalekseev")
class TestFailOperationAfterSuccessiveJobAbortsOnPrepareVolume(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": NUM_NODES,
            "default_file_replication_factor": NUM_NODES,
        }
    }

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
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "max_job_aborts_until_operation_failure": {
                "root_volume_preparation_failed": 2,
            },
        }
    }

    USE_PORTO = True

    def setup_files(self):
        create("file", "//tmp/squashfs.img")
        write_file("//tmp/squashfs.img", open("layers/squashfs.img", "rb").read())
        set("//tmp/squashfs.img/@access_method", "nbd")
        set("//tmp/squashfs.img/@filesystem", "squashfs")

    @authors("yuryalekseev")
    def test_abort_on_prepare_volume(self):
        self.setup_files()

        update_nodes_dynamic_config({
            "exec_node": {
                "nbd": {
                    "block_cache_compressed_data_capacity": 536870912,
                    "client": {
                        # Set read I/O timeout to 1 second
                        "io_timeout": 1000,
                        "connection_count": 2,
                    },
                    "enabled": True,
                    "server": {
                        "unix_domain_socket": {
                            # The best would be to use os.path.join(self.path_to_run, tempfile.mkstemp(dir="/tmp")[1]),
                            # but it leads to a path with length greater than the maximum allowed 108 bytes.
                            # So put it at home directory until PORTO-1242 is done, then put it in /tmp.
                            "path": tempfile.mkstemp(dir="/root" if os.environ["USER"] == "root" else "/home/" + os.environ["USER"])[1]
                        },
                    },
                },
                "volume_manager": {
                    "abort_on_operation_with_volume_failed": True,
                    "abort_on_operation_with_layer_failed": True,
                    "throw_on_prepare_volume": True,
                },
            },
        })

        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait_for_nodes()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="ls $YT_ROOT_FS/dir 1>&2",
                spec={
                    "mapper": {
                        "layer_paths": ["//tmp/squashfs.img"],
                    },
                },
            )

        # YT-14186: Corrupted user layer should not disable jobs on node.
        for node in ls("//sys/cluster_nodes"):
            assert len(get("//sys/cluster_nodes/{}/@alerts".format(node))) == 0

    @authors("yuryalekseev")
    def test_no_nbd_configuration(self):
        self.setup_files()

        update_nodes_dynamic_config({
            "exec_node": {
            },
        })

        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait_for_nodes()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"k": 0, "u": 1, "v": 2}])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="ls $YT_ROOT_FS/dir 1>&2",
                spec={
                    "mapper": {
                        "layer_paths": ["//tmp/squashfs.img"],
                    },
                },
            )

        # YT-14186: Corrupted user layer should not disable jobs on node.
        for node in ls("//sys/cluster_nodes"):
            assert len(get("//sys/cluster_nodes/{}/@alerts".format(node))) == 0


class TestEnableRootVolumeDiskQuota(YTEnvSetup):
    USE_PORTO = True

    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": True,
            },
        },
        "exec_node": {
            "slot_manager": {
                "do_not_set_user_id": True,
                "job_environment": {
                    "type": "porto",
                    "use_exec_from_layer": True,
                },
            },
        },
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_file_replication_factor": 1,
            "default_table_replication_factor": 1,
        },
    }

    def setup_files(self):
        create("file", "//tmp/exec.tar.gz")
        write_file("//tmp/exec.tar.gz", open("rootfs/exec.tar.gz", "rb").read())
        create("file", "//tmp/rootfs.tar.gz")
        write_file("//tmp/rootfs.tar.gz", open("rootfs/rootfs.tar.gz", "rb").read())

        create("file", "//tmp/sandbox.img", attributes={"filesystem": "squashfs", "access_method": "local"})
        write_file("//tmp/sandbox.img", open("layers/sandbox.img", "rb").read())

        create("file", "//tmp/mapper.sh", attributes={"executable": True})
        write_file("//tmp/mapper.sh", b"""echo {Hello=World}""")

        create("table", "//tmp/t_in1")
        write_table("//tmp/t_in1", {"foo": "bar"})

        create("table", "//tmp/t_in4", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t_in4", [{"a": i} for i in range(2)])

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")
        create("table", "//tmp/t_out3")
        create("table", "//tmp/t_out4")

    @authors("artemagafonov")
    @pytest.mark.timeout(150)
    def test_access_to_sandbox_in_layer(self):
        self.setup_files()

        map(
            in_="//tmp/t_in1",
            out="//tmp/t_out1",
            command="./mapper.sh",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/exec.tar.gz", "//tmp/rootfs.tar.gz", "//tmp/sandbox.img"],
                    # "disk_space_limit": 1024 * 1024,
                    "make_rootfs_writable": True,
                },
                "enable_root_volume_disk_quota": True,
            },
        )

        assert read_table("//tmp/t_out1") == [{"Hello": "World"}]

    @authors("artemagafonov")
    @pytest.mark.timeout(150)
    def test_copy_artifact_with_root_volume_disk_quota(self):
        self.setup_files()

        map(
            in_="//tmp/t_in1",
            out="//tmp/t_out2",
            command="./mapper.sh",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/exec.tar.gz", "//tmp/rootfs.tar.gz"],
                    "file_paths": ["//tmp/mapper.sh"],
                    "copy_files": True,
                    # "disk_space_limit": 1024 * 1024,
                    "make_rootfs_writable": True,
                },
                "enable_root_volume_disk_quota": True,
            },
        )

        assert read_table("//tmp/t_out2") == [{"Hello": "World"}]

        map(
            in_="//tmp/t_in1",
            out="//tmp/t_out3",
            command="./tmpfs/mapper.sh",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/exec.tar.gz", "//tmp/rootfs.tar.gz"],
                    "file_paths": [yson.to_yson_type("//tmp/mapper.sh", attributes={"file_name" : "tmpfs/mapper.sh"})],
                    "copy_files": True,
                    "tmpfs_path": "tmpfs",
                    "tmpfs_size": 1024 * 1024,
                    # "disk_space_limit": 1024 * 1024,
                    "make_rootfs_writable": True,
                },
                "enable_root_volume_disk_quota": True,
            },
        )

        assert read_table("//tmp/t_out3") == [{"Hello": "World"}]

    @authors("artemagafonov")
    @pytest.mark.timeout(150)
    def test_udf_exists(self):
        self.setup_files()

        map(
            in_="//tmp/t_in4",
            out="//tmp/t_out4",
            command="cat",
            mode="ordered",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/exec.tar.gz", "//tmp/rootfs.tar.gz"],
                    # "disk_space_limit": 1024 * 1024,
                    "make_rootfs_writable": True,
                },
                "enable_root_volume_disk_quota": True,
                "input_query": "a where a > 0"
            },
        )

        assert read_table("//tmp/t_out4") == [{"a": 1}]


@authors("artemagafonov")
class TestVirtualSandbox(YTEnvSetup):
    USE_PORTO = True

    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

    DELTA_NODE_CONFIG = {
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": True,
            },
        },
        "exec_node": {
            "slot_manager": {
                "do_not_set_user_id": True,
                "job_environment": {
                    "type": "porto",
                    "use_exec_from_layer": True,
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "nbd": {
                    "block_cache_compressed_data_capacity": 536870912,
                    "client": {
                        "io_timeout": 30000,
                    },
                    "enabled": True,
                    "server": {
                        "unix_domain_socket": {
                            # The best would be to use os.path.join(self.path_to_run, tempfile.mkstemp(dir="/tmp")[1]),
                            # but it leads to a path with length greater than the maximum allowed 108 bytes.
                            # So put it at home directory until PORTO-1242 is done, then put it in /tmp.
                            "path": tempfile.mkstemp(dir="/tmp" if "USER" not in os.environ else "/root" if os.environ["USER"] == "root" else "/home/" + os.environ["USER"])[1]
                        },
                    },
                },
            },
        }
    }

    def setup_files(self):
        create("file", "//tmp/exec.tar.gz")
        write_file("//tmp/exec.tar.gz", open("rootfs/exec.tar.gz", "rb").read())
        create("file", "//tmp/rootfs.tar.gz")
        write_file("//tmp/rootfs.tar.gz", open("rootfs/rootfs.tar.gz", "rb").read())

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        create("file", "//tmp/mapper.sh", attributes={"executable": True, "access_method": "nbd"})
        write_file("//tmp/mapper.sh", b"""echo {Hello=World}""")

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")
        create("table", "//tmp/t_out3")

    @authors("artemagafonov")
    @pytest.mark.timeout(300)
    def test_use_virtual_sandbox(self):
        self.setup_files()

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out1",
            command="./mapper.sh",
            file="//tmp/mapper.sh",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/exec.tar.gz", "//tmp/rootfs.tar.gz"],
                    # "disk_space_limit": 1024 * 1024,
                },
                "enable_root_volume_disk_quota": True,
                "enable_virtual_sandbox": True,
            },
        )

        assert read_table("//tmp/t_out1") == [{"Hello": "World"}]

    @authors("artemagafonov")
    @pytest.mark.timeout(300)
    def test_skip_files_inside_tmpfs(self):
        self.setup_files()

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out2",
            command="./mapper.sh",
            file="//tmp/mapper.sh",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/exec.tar.gz", "//tmp/rootfs.tar.gz"],
                    "tmpfs_path": ".",
                    "tmpfs_size": 1024 * 1024,
                    # "disk_space_limit": 1024 * 1024,
                },
                "enable_root_volume_disk_quota": True,
                "enable_virtual_sandbox": True,
            },
        )

        assert read_table("//tmp/t_out2") == [{"Hello": "World"}]

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out3",
            command="./tmpfs/mapper.sh",
            spec={
                "max_failed_job_count": 1,
                "mapper": {
                    "layer_paths": ["//tmp/exec.tar.gz", "//tmp/rootfs.tar.gz"],
                    "tmpfs_path": "tmpfs",
                    "file_paths": [yson.to_yson_type("//tmp/mapper.sh", attributes={"file_name" : "tmpfs/mapper.sh"})],
                    "tmpfs_size": 1024 * 1024,
                    # "disk_space_limit": 1024 * 1024,
                },
                "enable_root_volume_disk_quota": True,
                "enable_virtual_sandbox": True,
            },
        )

        assert read_table("//tmp/t_out3") == [{"Hello": "World"}]
