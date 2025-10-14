
from common import TestQueriesYqlBase

from yt_commands import authors
from yt.environment.components.yql_agent import YqlAgent as YqlAgentComponent

from yt.wrapper import yson

import yatest.common

import os
import re
import signal
import subprocess


class TestYqlAgentCrashHandler(TestQueriesYqlBase):
    NUM_YQL_AGENTS = 0
    NUM_NODES = 1
    NUM_HTTP_PROXIES = 1
    NUM_RPC_PROXIES = 1
    NUM_SCHEDULERS = 1

    @authors("aleksandr.gaev")
    def test_emits_stacktrace_on_segmentation_fault(self):
        component = YqlAgentComponent()
        config = {
            "count": 1,
            "path": yatest.common.binary_path("yt/yql/agent/bin/ytserver-yql-agent"),
            "mr_job_bin": yatest.common.binary_path("yt/yql/tools/mrjob/mrjob"),
            "mr_job_udfs_dir": os.path.dirname(yatest.common.binary_path("yql/essentials/udfs/common")),
            "yql_plugin_shared_library": yatest.common.binary_path("yt/yql/plugin/dynamic/libyqlplugin.so"),
            "ui_origin": "https://localhost",
            "native_client_supported": True,
            "libraries": {},
        }
        component.prepare(self.Env, config)

        config_path = component.config_paths[0]
        with open(config_path, "rb") as config_file:
            cfg = yson.load(config_file)
        cfg.setdefault("yql_agent", {})["core_dumper"] = yson.YsonEntity()

        with open(config_path, "wb") as config_file:
            yson.dump(cfg, config_file, yson_format="text")

        binary = yatest.common.binary_path("yt/yql/agent/bin/ytserver-yql-agent")
        proc = subprocess.Popen([binary, "--config", config_path], stderr=subprocess.PIPE)

        component.wait_for_readiness(component.addresses[0])

        proc.send_signal(signal.SIGSEGV)
        _, stderr = proc.communicate(timeout=30)
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        assert "SIGSEGV (Segmentation violation)" in stderr
        assert "*** Waiting for logger to shut down" in stderr
        assert re.search(r"\b\d{2}\. 0x[0-9a-fA-F]+", stderr), stderr
