from .conftest import get_tests_sandbox

from yt.testlib import authors
from yt.clickhouse.test_helpers import (get_host_paths, create_symlinks_for_chyt_binaries,
                                        create_controller_init_cluster_config, create_controller_run_config)

from yt.common import wait

from yt.wrapper.strawberry import describe_api

import yt.wrapper as yt
import yt.clickhouse as chyt

from yt.environment.helpers import OpenPortIterator
import yt.environment.arcadia_interop as arcadia_interop

import yt.packages.requests as requests

import os.path
import pytest

import yatest

HOST_PATHS = get_host_paths(arcadia_interop, ["ytserver-clickhouse", "clickhouse-trampoline", "chyt-controller"])


def check_controller_api_readiness():
    try:
        describe_api(os.environ["CHYT_CTL_ADDRESS"], "chyt", "test_stage")
        return True
    except requests.exceptions.ConnectionError:
        return False


def get_work_dir():
    return get_tests_sandbox() + "/test_controller"


@pytest.mark.usefixtures("yt_env")
class TestController(object):
    def setup_class(cls):
        os.mkdir(get_work_dir())
        create_symlinks_for_chyt_binaries(HOST_PATHS, get_work_dir())

    def setup(self):
        cluster_proxy = yt.config.config["proxy"]["url"]

        open_port_iterator = OpenPortIterator()
        controller_api_port = next(open_port_iterator)

        os.environ["YT_TEST_USER"] = "root"
        os.environ["CHYT_CTL_ADDRESS"] = "http://localhost:{}".format(controller_api_port)

        work_dir = get_work_dir()
        init_cluster_config_path = create_controller_init_cluster_config(cluster_proxy, work_dir, ["chyt"])
        run_config_path = create_controller_run_config(
            proxy=cluster_proxy,
            work_dir=work_dir,
            api_port=controller_api_port,
            monitoring_port=next(open_port_iterator))

        yatest.common.execute(
            [
                HOST_PATHS["chyt-controller"],
                "--config-path", init_cluster_config_path,
                "init-cluster",
            ],
            wait=True)

        self.controller_process = yatest.common.execute(
            [
                HOST_PATHS["chyt-controller"],
                "--config-path", run_config_path,
                "--log-to-stderr",
                "run",
            ],
            wait=False)

        wait(check_controller_api_readiness)

        self.client = chyt.ChytClient()

    def teardown(self):
        if self.controller_process:
            self.controller_process.kill()

    @authors("gudqeit")
    def test_create(self):
        self.client.create(alias="a")
        assert "a" in self.client.list()

    @authors("gudqeit")
    def test_remove(self):
        self.client.create(alias="b")
        self.client.remove(alias="b")
        assert "b" not in self.client.list()

    @authors("gudqeit")
    def test_exists(self):
        self.client.create(alias="c")
        assert self.client.exists(alias="c")

    @authors("gudqeit")
    def test_status(self):
        self.client.create(alias="d")
        assert self.client.status(alias="d")["health"] == "good"

    @authors("gudqeit")
    def test_get_option(self):
        self.client.create(alias="e")
        assert self.client.get_option(alias="e", key="stage") == "test"

    @authors("gudqeit")
    def test_set_option(self):
        self.client.create(alias="f")
        self.client.set_option(alias="f", key="test_option", value=1234)
        assert self.client.get_option(alias="f", key="test_option") == 1234

    @authors("gudqeit")
    def test_remove_option(self):
        self.client.create(alias="g")
        self.client.set_option(alias="g", key="test_option", value=1234)
        self.client.remove_option(alias="g", key="test_option")
        with pytest.raises(yt.YtError):
            self.client.get_option(alias="g", key="test_option")

    @authors("gudqeit")
    def test_get_speclet(self):
        self.client.create(alias="h")
        assert self.client.get_speclet(alias="h") == {"family": "chyt", "stage": "test"}

    @authors("gudqeit")
    def test_set_speclet(self):
        self.client.create(alias="i")
        self.client.set_speclet(alias="i", speclet={
            "test_option": 1234,
        })
        assert self.client.get_speclet(alias="i") == {
            "family": "chyt",
            "stage": "test",
            "test_option": 1234,
        }

    @authors("gudqeit")
    def test_stop(self):
        self.client.create(alias="j")
        self.client.stop(alias="j")
        assert not self.client.get_option(alias="j", key="active")
