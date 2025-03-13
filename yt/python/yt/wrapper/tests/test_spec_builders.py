from .conftest import authors
from .helpers import TEST_DIR, check_rows_equality, get_test_file_path, set_config_option, get_python

from yt.wrapper.common import update, get_started_by
from yt.wrapper.driver import get_api_version
from yt.wrapper.spec_builders import (ReduceSpecBuilder, MergeSpecBuilder, SortSpecBuilder,
                                      MapReduceSpecBuilder, MapSpecBuilder, VanillaSpecBuilder)
from yt.wrapper.spec_builder_helpers import BaseLayerDetector, distro, platform  # noqa

try:
    from yt.python.yt.cpp_wrapper import CppJob
    _CPP_WRAPPER_AVAILABLE = True
except ImportError:
    _CPP_WRAPPER_AVAILABLE = False

import yt.wrapper as yt

import mock
import pytest

from copy import deepcopy
import sys


class NonCopyable:
    def __init__(self, fun):
        self._fun = fun

    def __call__(self, *args, **kwargs):
        return self._fun(*args, **kwargs)

    def __deepcopy__(self, _memo):
        raise TypeError("not copyable")

    def __copy__(self):
        raise TypeError("not copyable")


@pytest.mark.usefixtures("yt_env")
class TestSpecBuilders(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()

    def teardown(self):
        yt.config["tabular_data_format"] = None
        yt.remove("//tmp/yt_wrapper/file_storage", recursive=True, force=True)

    @authors("ignat")
    def test_merge(self):
        tableX = TEST_DIR + "/tableX"
        tableY = TEST_DIR + "/tableY"
        dir = TEST_DIR + "/dir"
        res_table = dir + "/other_table"

        yt.write_table(tableX, [{"x": 1}])
        yt.write_table(tableY, [{"y": 2}])

        with pytest.raises(yt.YtError):
            spec_builder = MergeSpecBuilder() \
                .input_table_paths([tableX, tableY]) \
                .output_table_path(res_table) # noqa
            yt.run_operation(spec_builder)
        with pytest.raises(yt.YtError):
            spec_builder = MergeSpecBuilder() \
                .input_table_paths([tableX, tableY]) \
                .output_table_path(res_table) # noqa
            yt.run_operation(spec_builder)

        yt.mkdir(dir)
        spec_builder = MergeSpecBuilder() \
            .input_table_paths([tableX, tableY]) \
            .output_table_path(res_table) # noqa
        yt.run_operation(spec_builder)
        check_rows_equality([{"x": 1}, {"y": 2}], yt.read_table(res_table), ordered=False)

        spec_builder = MergeSpecBuilder() \
            .input_table_paths(tableX) \
            .output_table_path(res_table) # noqa
        yt.run_operation(spec_builder)
        assert not yt.get_attribute(res_table, "sorted")
        check_rows_equality([{"x": 1}], yt.read_table(res_table))

        spec_builder = SortSpecBuilder() \
            .input_table_paths(tableX) \
            .sort_by(["x"]) \
            .output_table_path(tableX) # noqa
        yt.run_operation(spec_builder)
        spec_builder = MergeSpecBuilder() \
            .input_table_paths(tableX) \
            .output_table_path(res_table) \
            .mode("sorted") \
            .begin_job_io() \
                .table_writer({"desired_chunk_size": 1024}) \
            .end_job_io() # noqa
        yt.run_operation(spec_builder)
        assert yt.get_attribute(res_table, "sorted")
        check_rows_equality([{"x": 1}], yt.read_table(res_table))

    @authors("ignat")
    def test_run_operation(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        spec_builder = MapSpecBuilder() \
            .begin_mapper() \
                .command("cat") \
                .format("json") \
            .end_mapper() \
            .input_table_paths(table) \
            .output_table_paths(table) \
            .ordered(False) # noqa
        yt.run_operation(spec_builder)
        check_rows_equality([{"x": 1}, {"x": 2}], list(yt.read_table(table)), ordered=False)
        spec_builder = SortSpecBuilder() \
            .input_table_paths(table) \
            .sort_by(["x"]) \
            .output_table_path(table) # noqa
        yt.run_operation(spec_builder)

        # with pytest.raises(yt.YtError):
        #     spec_builder = ReduceSpecBuilder() \
        #         .begin_reducer() \
        #             .command("cat") \
        #             .format("json") \
        #         .end_reducer() \
        #         .input_table_paths(table) \
        #         .output_table_paths([]) \
        #         .reduce_by(["x"])
        #     yt.run_operation(spec_builder)

        spec_builder = ReduceSpecBuilder() \
            .begin_reducer() \
                .command("cat") \
                .format("json") \
            .end_reducer() \
            .input_table_paths(table) \
            .output_table_paths(table) \
            .reduce_by(["x"]) # noqa
        yt.run_operation(spec_builder)
        check_rows_equality([{"x": 1}, {"x": 2}], yt.read_table(table))

        spec_builder = MapSpecBuilder() \
            .begin_mapper() \
                .command("grep 2") \
                .format("json") \
            .end_mapper() \
            .input_table_paths(table) \
            .output_table_paths(other_table) # noqa
        yt.run_operation(spec_builder)
        check_rows_equality([{"x": 2}], yt.read_table(other_table))

        with pytest.raises(yt.YtError):
            spec_builder = MapSpecBuilder() \
                .begin_mapper() \
                    .command("cat") \
                    .format("json") \
                .end_mapper() \
                .input_table_paths([table, table + "xxx"]) \
                .output_table_paths(other_table) # noqa
            yt.run_operation(spec_builder)

        with pytest.raises(yt.YtError):
            spec_builder = ReduceSpecBuilder() \
                .begin_reducer() \
                    .command("cat") \
                    .format("json") \
                .end_reducer() \
                .input_table_paths(table) \
                .output_table_paths(other_table) # noqa
            yt.run_operation(spec_builder)

        # Run reduce on unsorted table
        with pytest.raises(yt.YtError):
            spec_builder = ReduceSpecBuilder() \
                .begin_reducer() \
                    .command("cat") \
                    .format("json") \
                .end_reducer() \
                .input_table_paths(other_table) \
                .output_table_paths(table) \
                .reduce_by(["x"]) # noqa
            yt.run_operation(spec_builder)

        yt.write_table(table,
                       [
                           {"a": 12, "b": "ignat"},
                           {"b": "max"},
                           {"a": "x", "b": "name", "c": 0.5}
                       ])
        spec_builder = MapSpecBuilder() \
            .begin_mapper() \
                .command("PYTHONPATH=. {} capitalize_b.py".format(get_python())) \
                .file_paths(yt.LocalFile(get_test_file_path("capitalize_b.py"))) \
                .format(yt.DsvFormat()) \
            .end_mapper() \
            .input_table_paths(yt.TablePath(table, columns=["b"])) \
            .output_table_paths(other_table) # noqa
        yt.run_operation(spec_builder)
        records = yt.read_table(other_table, raw=False)
        assert sorted([rec["b"] for rec in records]) == ["IGNAT", "MAX", "NAME"]
        assert sorted([rec["c"] for rec in records]) == []

        import pathlib
        spec_builder = MapSpecBuilder() \
            .begin_mapper() \
                .command("PYTHONPATH=. {} capitalize_b.py".format(get_python())) \
                .file_paths(yt.LocalFile(pathlib.Path(get_test_file_path("capitalize_b.py")))) \
                .format(yt.DsvFormat()) \
            .end_mapper() \
            .input_table_paths(yt.TablePath(table, columns=["b"])) \
            .output_table_paths(other_table) # noqa
        yt.run_operation(spec_builder)
        records = yt.read_table(other_table, raw=False)
        assert sorted([rec["b"] for rec in records]) == ["IGNAT", "MAX", "NAME"]
        assert sorted([rec["c"] for rec in records]) == []

    @authors("ignat")
    def test_reduce_combiner(self):
        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(table, [{"x": 1}, {"y": 2}])

        spec_builder = MapReduceSpecBuilder() \
            .begin_reduce_combiner() \
                .command("cat") \
            .end_reduce_combiner() \
            .begin_reducer() \
                .command("cat") \
            .end_reducer() \
            .reduce_by(["x"]) \
            .sort_by(["x"]) \
            .input_table_paths(table) \
            .output_table_paths(output_table) # noqa

        yt.run_operation(spec_builder)
        check_rows_equality([{"x": 1}, {"y": 2}], list(yt.read_table(table)))

    @authors("asaitgalin")
    def test_preserve_user_spec_between_invocations(self):
        input_ = TEST_DIR + "/input"
        output = TEST_DIR + "/output"
        yt.write_table(input_, [{"x": 1}, {"y": 2}])
        spec = {"weight": 100.0, "title": "Test operation"}

        yt.run_sort(input_, input_, sort_by=["x"], spec=spec)
        yt.run_map("cat", input_, output, format="json", spec=spec)
        yt.run_map_reduce(None, "cat", input_, output, reduce_by=["x"], format="json", spec=spec)

    @authors("asaitgalin")
    def test_spec_overrides_and_defaults(self):
        input_table = TEST_DIR + "/input"
        output_table = TEST_DIR + "/output"
        yt.write_table(input_table, [{"x": 1}, {"y": 2}])

        spec_defaults = {
            "mapper": {
                "memory_limit": 128 * 1024 * 1024,
                "environment": {
                    "var1": "1",
                    "var2": "2"}}}

        spec_overrides = {
            "mapper": {
                "cpu_limit": 0.5772156649,
                "memory_reserve_factor": 0.31,
                "environment": {
                    "var2": "5",
                    "var3": "6"}}}

        with set_config_option("spec_defaults", spec_defaults):
            with set_config_option("spec_overrides", spec_overrides):
                spec_builder = MapSpecBuilder() \
                    .begin_mapper() \
                        .command("cat") \
                        .format("json") \
                        .cpu_limit(1.0) \
                        .memory_limit(256 * 1024 * 1024) \
                    .end_mapper() \
                    .input_table_paths(input_table) \
                    .output_table_paths(output_table) # noqa

                op = yt.run_operation(spec_builder)
                attributes = op.get_attributes()
                assert attributes["spec"]["mapper"]["memory_limit"] == 256 * 1024 * 1024
                assert abs(attributes["spec"]["mapper"]["cpu_limit"] - 1.0) < 1e-5
                assert abs(attributes["spec"]["mapper"]["memory_reserve_factor"] - 0.31) < 1e-5
                assert attributes["spec"]["mapper"]["environment"]["var1"] == "1"
                assert attributes["spec"]["mapper"]["environment"]["var2"] == "5"
                assert attributes["spec"]["mapper"]["environment"]["var3"] == "6"

    @authors("prime")
    def test_user_job_spec_defaults(self):
        input_table = TEST_DIR + "/input"
        output_table = TEST_DIR + "/output"
        yt.write_table(yt.TablePath(input_table, sorted_by=["x"]), [{"x": 1}, {"x": 2, "y": 2}])

        user_job_spec_defaults = {
            "environment": {
                "MY_ENV": "value",
            }
        }

        with set_config_option("user_job_spec_defaults", user_job_spec_defaults):
            spec_builder = MapSpecBuilder() \
                .begin_mapper() \
                    .command("cat") \
                    .format("json") \
                    .memory_limit(256 * 1024 * 1024) \
                .end_mapper() \
                .input_table_paths(input_table) \
                .output_table_paths(output_table) # noqa
            op = yt.run_operation(spec_builder)
            attributes = op.get_attributes()
            assert attributes["spec"]["mapper"]["memory_limit"] == 256 * 1024 * 1024
            assert attributes["spec"]["mapper"]["environment"]["MY_ENV"] == "value"

            spec_builder = ReduceSpecBuilder() \
                .begin_reducer() \
                    .command("cat") \
                    .format("json") \
                    .memory_limit(256 * 1024 * 1024) \
                    .environment({"OTHER_ENV": "10"}) \
                .end_reducer() \
                .reduce_by(["x"]) \
                .input_table_paths(input_table) \
                .output_table_paths(output_table) # noqa
            op = yt.run_operation(spec_builder)
            attributes = op.get_attributes()
            assert attributes["spec"]["reducer"]["memory_limit"] == 256 * 1024 * 1024
            assert attributes["spec"]["reducer"]["environment"]["MY_ENV"] == "value"
            assert attributes["spec"]["reducer"]["environment"]["OTHER_ENV"] == "10"

    @authors("ignat")
    def test_spec_deepcopy(self):
        def mapper(row):
            yield {"x": row["x"] + 5, "tag": row["tag"]}
        mapper = NonCopyable(mapper)

        def reducer(key, rows):
            total = sum(row["x"] for row in rows)
            yield {"total": total, "tag": key["tag"]}
        reducer = NonCopyable(reducer)

        with pytest.raises(TypeError):
            deepcopy(mapper)
        with pytest.raises(TypeError):
            deepcopy(reducer)

        input_table = TEST_DIR + "/input"
        output_table = TEST_DIR + "/output"
        yt.write_table(input_table, [
            {"x": 3, "tag": "Paris"},
            {"x": 2, "tag": "Paris"},
            {"x": 4, "tag": "London"},
            {"x": 8, "tag": "London"},
        ])

        spec_builder = MapReduceSpecBuilder() \
            .begin_mapper() \
                .command(mapper) \
            .end_mapper() \
            .begin_reducer() \
                .command(reducer) \
            .end_reducer() \
            .reduce_by("tag") \
            .input_table_paths(input_table) \
            .output_table_paths(output_table) # noqa

        yt.run_operation(spec_builder)
        check_rows_equality(
            [
                {"tag": "Paris", "total": 15},
                {"tag": "London", "total": 22},
            ],
            list(yt.read_table(output_table)),
            ordered=False
        )

    @authors("prime")
    def test_vanilla_spec_builder(self):
        vanilla_spec = VanillaSpecBuilder() \
            .begin_task("sample") \
                .command("cat") \
                .job_count(1) \
            .end_task() \
            .spec({"tasks": {"sample": {"memory_limit": 666 * 1024}}, "weight": 2}) # noqa

        result_spec = vanilla_spec.build()
        correct_spec = {
            "tasks": {
                "sample": {
                    "command": "cat",
                    "job_count": 1,
                    "memory_limit": 666 * 1024,
                }
            },
            "weight": 2,
        }

        assert update(result_spec, correct_spec) == result_spec

    @authors("max42")
    def test_allow_http_requests_to_yt_from_job_vanilla(self):
        with set_config_option("allow_http_requests_to_yt_from_job", True):
            spec_builder = VanillaSpecBuilder().spec({
                "tasks": {
                    "script": {
                        "command": "cat",
                        "job_count": 1,
                    }
                }
            })
            result_spec1 = spec_builder.build()

        result_spec2 = VanillaSpecBuilder().spec({}).task("script", {
            "environment": {"YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB": "1"},
            "command": "cat",
            "job_count": 1,
        }).build()

        correct_spec = {
            "tasks": {
                "script": {
                    "command": "cat",
                    "job_count": 1,
                    "environment": {
                        "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB": "1",
                    }
                }
            }
        }

        assert update(result_spec1, correct_spec) == result_spec1
        assert update(result_spec2, correct_spec) == result_spec2

    @authors("max42")
    def test_allow_http_requests_to_yt_from_job_map(self):
        with set_config_option("allow_http_requests_to_yt_from_job", True):
            spec_builder = MapSpecBuilder().spec({
                "mapper": {
                    "command": "cat",
                },
                "input_table_paths": ["//tmp/t_in"],
                "output_table_paths": ["//tmp/t_out"],
            })
            result_spec1 = spec_builder.build()

        result_spec2 = MapSpecBuilder() \
            .begin_mapper() \
                .command("cat") \
                .environment({"YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB": "1"}) \
            .end_mapper() \
            .input_table_paths(["//tmp/t_in"]) \
            .output_table_paths(["//tmp/t_out"]) \
            .build() # noqa

        correct_spec = {
            "mapper": {
                "command": "cat",
                "environment": {
                    "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB": "1",
                }
            },
            "input_table_paths": ["//tmp/t_in"],
            "output_table_paths": ["//tmp/t_out"],
        }

        assert update(result_spec1, correct_spec) == result_spec1
        assert update(result_spec2, correct_spec) == result_spec2

    @authors("levysotsky")
    def test_spec_builder_started_by_truncation(self):
        vanilla_spec = VanillaSpecBuilder() \
            .begin_task("sample") \
                .command("cat") \
                .job_count(1) \
            .end_task() \
            .spec({"tasks": {"sample": {"memory_limit": 666 * 1024}}, "weight": 2}) # noqa

        with set_config_option("started_by_command_length_limit", 4096):
            result_spec = deepcopy(vanilla_spec).build()
            assert result_spec["started_by"]["command"] == get_started_by()["command"]

        limit = len(sys.argv[0]) + 1
        with set_config_option("started_by_command_length_limit", limit):
            result_spec = deepcopy(vanilla_spec).build()
            assert result_spec["started_by"]["command"] == [sys.argv[0], sys.argv[1][0] + "...truncated"]

        with set_config_option("started_by_command_length_limit", 0):
            result_spec = deepcopy(vanilla_spec).build()
            assert "command" not in result_spec["started_by"]

    @authors("aleexfi")
    def test_human_readable_operation_title(self):
        spec = MapSpecBuilder() \
            .begin_mapper() \
                .command("cat") \
            .end_mapper() \
            .input_table_paths(["//tmp/t_in"]) \
            .output_table_paths(["//tmp/t_out"]) \
            .build() # noqa

        assert "title" in spec
        assert "Mapper" in spec["title"] and "cat" in spec["title"]

        spec = VanillaSpecBuilder() \
            .begin_task("sample") \
                .command("cat") \
                .job_count(1) \
            .end_task() \
            .build() # noqa

        assert "title" in spec
        assert "cat" in spec["title"] and "Sample" in spec["title"]

        if _CPP_WRAPPER_AVAILABLE:
            spec = MapReduceSpecBuilder() \
                .begin_reduce_combiner() \
                    .command(CppJob("cat")) \
                .end_reduce_combiner() \
                .begin_reducer() \
                    .command(CppJob("cat")) \
                .end_reducer() \
                .reduce_by(["x"]) \
                .sort_by(["x"]) \
                .build() # noqa

            assert "title" in spec
            for part in ["Mapper", "Reducer", "cat"]:
                assert part in spec["title"]

    @authors("denvr")
    def test_porto_auto_layer(self):

        if get_api_version() != "v4":
            pytest.skip()

        client = yt.YtClient(config=yt.config.config)

        with mock.patch("distro.id", return_value="ubuntu"):
            with mock.patch("platform.python_version_tuple", return_value=("3", "6", "1",)), \
                    mock.patch("distro.codename", return_value="bionic"):
                assert BaseLayerDetector._detect_os_env() == ("ubuntu", "bionic", None), "Native python"

            with mock.patch("platform.python_version_tuple", return_value=("3", "8", "1",)), \
                    mock.patch("distro.codename", return_value="bionic"):
                assert BaseLayerDetector._detect_os_env() == ("ubuntu", "bionic", (3, 8)), "Newer python"

            with mock.patch("platform.python_version_tuple", return_value=("3", "12", "1",)), \
                    mock.patch("distro.codename", return_value="obvious"):
                assert BaseLayerDetector._detect_os_env() == ("ubuntu", "obvious", (3, 12)), "Unknown os"

        # newer python version
        with mock.patch("distro.id", return_value="ubuntu"), \
                mock.patch("platform.python_version_tuple", return_value=("3", "11", "1",)), \
                mock.patch("distro.codename", return_value="bionic"):

            assert not client.exists(client.config["base_layers_registry_path"])

            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") is None, "no registry, no regular"

            client.create("file", "//porto_layers/base/focal/porto_layer_search_ubuntu_focal_app_lastest.tar.gz", recursive=True)
            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") is None, "no registry, wrong regular"

            client.create("file", "//porto_layers/base/bionic/porto_layer_search_ubuntu_bionic_app_lastest.tar.gz", recursive=True)
            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") == ["//porto_layers/base/bionic/porto_layer_search_ubuntu_bionic_app_lastest.tar.gz"], \
                "regular, wrong python"

            # with registry
            client.create("map_node", client.config["base_layers_registry_path"], recursive=True)
            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") == ["//porto_layers/base/bionic/porto_layer_search_ubuntu_bionic_app_lastest.tar.gz"], \
                "regular, wrong python, wrong registry"
            client.remove(client.config["base_layers_registry_path"])

            client.create("document", client.config["base_layers_registry_path"], recursive=True)
            client.set(
                client.config["base_layers_registry_path"],
                ["", ""]
            )
            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") == ["//porto_layers/base/bionic/porto_layer_search_ubuntu_bionic_app_lastest.tar.gz"], \
                "regular, wrong python, wrong registry"

            client.set(
                client.config["base_layers_registry_path"],
                {
                    "porto": {
                        "//tmp/l1": {"os": "ubuntu", "base_layers": None, "tags": ""}
                    }
                }
            )
            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") == ["//porto_layers/base/bionic/porto_layer_search_ubuntu_bionic_app_lastest.tar.gz"], \
                "regular, wrong python, wrong registry"
            assert BaseLayerDetector._get_default_layer(client, layer_type="docker") == [], \
                "no match"

            client.set(
                client.config["base_layers_registry_path"],
                {
                    "porto": {
                        "//tmp/l_bionic_3.10": {"os": "ubuntu", "base_layers": None, "tags": ["python=3.10", "os_codename=bionic"]},
                    },
                    "docker": {
                        "images/docker/image1": {"os": "ubuntu", "tags": ["python=default", "os_codename=bionic"]},
                        "registry.cluster_name.yandex.net/images/docker/image1:tag1": {"os": "ubuntu", "tags": ["python=3.10", "os_codename=bionic"]}
                    }
                }
            )
            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") == ["//porto_layers/base/bionic/porto_layer_search_ubuntu_bionic_app_lastest.tar.gz"], \
                "wrong python, use default form //porto_layers"
            assert BaseLayerDetector._get_default_layer(client, layer_type="docker") == ["images/docker/image1"], \
                "wrong python, use default from registry"

            client.set(
                client.config["base_layers_registry_path"],
                {
                    "porto": {
                        "//tmp/l_bionic_latest": {"os": "ubuntu", "base_layers": None, "tags": ["python=default", "os_codename=bionic"]},
                        "//tmp/l_bionic_3.10": {"os": "ubuntu", "base_layers": None, "tags": ["python=3.10", "os_codename=bionic"]},
                        "//tmp/l_bionic_3.11": {"os": "ubuntu", "base_layers": ["//tmp/base1"], "tags": ["python=3.11", "os_codename=bionic"]},
                    },
                    "docker": {
                        "images/docker/image1:latest": {"os": "ubuntu", "tags": ["python=default", "os_codename=bionic"]},
                        "registry.cluster_name.yandex.net/images/docker/image1:py310": {"os": "ubuntu", "tags": ["python=3.10", "os_codename=bionic"]},
                        "registry.cluster_name.yandex.net/images/docker/image1:py311": {"os": "ubuntu", "tags": ["python=3.11", "os_codename=bionic"]}
                    }
                }
            )

            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") == ["//porto_layers/base/bionic/porto_layer_search_ubuntu_bionic_app_lastest.tar.gz"], \
                "regular, wrong python, missed files, fallback"
            client.create("file", "//tmp/l_bionic_3.10", recursive=True)
            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") == ["//porto_layers/base/bionic/porto_layer_search_ubuntu_bionic_app_lastest.tar.gz"], \
                "regular, wrong python, missed registry file, fallback"
            client.create("file", "//tmp/l_bionic_3.11", recursive=True)
            client.create("file", "//tmp/l_bionic_latest", recursive=True)
            client.create("file", "//tmp/base1", recursive=True)
            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") == ["//tmp/l_bionic_3.11", "//tmp/base1"], \
                "ok"
            assert BaseLayerDetector._get_default_layer(client, layer_type="docker") == ["registry.cluster_name.yandex.net/images/docker/image1:py311"], \
                "ok"

            # interaction with operation
            assert client.config["operation_base_layer"] is None
            spec = MapSpecBuilder() \
                .begin_mapper() \
                    .layer_paths(["//some_path"]) \
                    .command("cat") \
                .end_mapper() \
                .input_table_paths(["//tmp/t_in"]).output_table_paths(["//tmp/t_out"]) \
                .build(client=client)  # noqa
            assert spec["mapper"]["layer_paths"] == ["//some_path"], "regular operation"

            spec = MapSpecBuilder() \
                .begin_mapper() \
                    .command("cat") \
                .end_mapper() \
                .input_table_paths(["//tmp/t_in"]).output_table_paths(["//tmp/t_out"]) \
                .build(client=client)  # noqa
            assert "layer_paths" not in spec["mapper"], "regular operation"

            client.config["operation_base_layer"] = "porto:auto"

            spec = MapSpecBuilder() \
                .begin_mapper() \
                    .layer_paths(["//some_path"]) \
                    .command("cat") \
                .end_mapper() \
                .input_table_paths(["//tmp/t_in"]).output_table_paths(["//tmp/t_out"]) \
                .build(client=client)  # noqa
            assert spec["mapper"]["layer_paths"] == ["//some_path"], "regular operation - keep original layer"
            spec = MapSpecBuilder() \
                .begin_mapper() \
                    .command("cat") \
                .end_mapper() \
                .input_table_paths(["//tmp/t_in"]).output_table_paths(["//tmp/t_out"]) \
                .build(client=client)  # noqa
            assert spec["mapper"]["layer_paths"] == ["//tmp/l_bionic_3.11", "//tmp/base1"], "guess base layer"
            assert "docker_image" not in spec["mapper"]

            client.config["operation_base_layer"] = "docker:auto"

            spec = MapSpecBuilder() \
                .begin_mapper() \
                    .docker_image("some_image") \
                    .command("cat") \
                .end_mapper() \
                .input_table_paths(["//tmp/t_in"]).output_table_paths(["//tmp/t_out"]) \
                .build(client=client)  # noqa
            assert spec["mapper"]["docker_image"] == "some_image", "regular operation - keep original layer"
            spec = MapSpecBuilder() \
                .begin_mapper() \
                    .command("cat") \
                .end_mapper() \
                .input_table_paths(["//tmp/t_in"]).output_table_paths(["//tmp/t_out"]) \
                .build(client=client)  # noqa
            assert "layer_paths" not in spec["mapper"]
            assert spec["mapper"]["docker_image"] == "registry.cluster_name.yandex.net/images/docker/image1:py311", "guess base layer"

            # user layers
            spec = MapSpecBuilder() \
                .begin_mapper() \
                    .command("cat") \
                .end_mapper() \
                .input_table_paths(["//tmp/t_in"]).output_table_paths(["//tmp/t_out"])  # noqa
            client.config["operation_base_layer"] = "//layer1"
            assert deepcopy(spec).build(client=client)["mapper"]["layer_paths"] == ["//layer1"]
            client.config["operation_base_layer"] = "  //layer1,  //layer2  "
            assert deepcopy(spec).build(client=client)["mapper"]["layer_paths"] == ["//layer1", "//layer2"], "split layers"
            client.config["operation_base_layer"] = "registry.cluster_name.yandex.net/yt/storage/some_image"
            assert deepcopy(spec).build(client=client)["mapper"]["docker_image"] == "registry.cluster_name.yandex.net/yt/storage/some_image", "docker: host path"
            client.config["operation_base_layer"] = "yt/storage/some_image:latest"
            assert deepcopy(spec).build(client=client)["mapper"]["docker_image"] == "yt/storage/some_image:latest", "docker: local path"
            client.config["operation_base_layer"] = "docker.io/some_image:latest"
            assert deepcopy(spec).build(client=client)["mapper"]["docker_image"] == "docker.io/some_image:latest", "external docker: as is"

        # native python version
        with mock.patch("distro.id", return_value="ubuntu"), \
                mock.patch("platform.python_version_tuple", return_value=("3", "6", "1",)), \
                mock.patch("distro.codename", return_value="bionic"):

            assert client.config["base_layers_registry_path"] == "//images/base_layers"

            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") == ["//tmp/l_bionic_latest"], \
                "ok, from registry"
            assert BaseLayerDetector._get_default_layer(client, layer_type="docker") == ["images/docker/image1:latest"], \
                "ok, from registry"

            client.config["base_layers_registry_path"] = "//wrong_path"

            assert BaseLayerDetector._get_default_layer(client, layer_type="porto") == ["//porto_layers/base/bionic/porto_layer_search_ubuntu_bionic_app_lastest.tar.gz"], \
                "ok, from //porto_layers"
            assert BaseLayerDetector._get_default_layer(client, layer_type="docker") == [], \
                "no docker default image without registry"
