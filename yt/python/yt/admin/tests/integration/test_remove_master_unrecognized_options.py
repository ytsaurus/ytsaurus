from yt.admin.remove_master_unrecognized_options import run_remove_master_unrecognized_options
import yt.wrapper

from yt_commands import authors, get, set, exists, remove, wait
from yt_env_setup import YTEnvSetup

import pytest

from typing import Generator


UNRECOGNIZED_OPTION_PATH = "//sys/@config/this_option_does_not_exist"


class TestRemoveMasterUnrecognizedOptions(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1
    NUM_MASTERS = 1
    NUM_NODES = 0

    @pytest.fixture
    def yt_client(self) -> Generator[yt.wrapper.YtClient, None, None]:
        yield self.Env.create_client()

    def teardown_method(self, method):
        if exists(UNRECOGNIZED_OPTION_PATH):
            remove(UNRECOGNIZED_OPTION_PATH)
            wait(lambda: len(get("//sys/@master_alerts")) == 0)
        super(TestRemoveMasterUnrecognizedOptions, self).teardown_method(method)

    @authors("ilyaibraev")
    def test_removes_unrecognized_option(self, yt_client):
        set(UNRECOGNIZED_OPTION_PATH, "some_value")
        wait(lambda: len(get("//sys/@master_alerts")) == 1)
        assert exists(UNRECOGNIZED_OPTION_PATH)

        run_remove_master_unrecognized_options(dry=False, do_not_print_config=True, client=yt_client)

        assert not exists(UNRECOGNIZED_OPTION_PATH)
        wait(lambda: len(get("//sys/@master_alerts")) == 0)

    @authors("ilyaibraev")
    def test_dry_run_keeps_option(self, yt_client):
        set(UNRECOGNIZED_OPTION_PATH, "some_value")
        wait(lambda: len(get("//sys/@master_alerts")) == 1)

        run_remove_master_unrecognized_options(dry=True, do_not_print_config=True, client=yt_client)

        assert exists(UNRECOGNIZED_OPTION_PATH)
        assert len(get("//sys/@master_alerts")) == 1

    @authors("ilyaibraev")
    def test_no_unrecognized_options_is_noop(self, yt_client):
        assert get("//sys/@master_alerts") == []

        run_remove_master_unrecognized_options(dry=False, do_not_print_config=True, client=yt_client)

        assert get("//sys/@master_alerts") == []
