import pytest
import sys

from copy import deepcopy

from .conftest import authors

import yt.wrapper as yt


@pytest.mark.usefixtures("yt_env_with_authentication_rpc")
class TestAuthorizedFeatures:

    @authors("denvr")
    def test_use_switch_to_local_rpc_proxy(self):
        cluster_name = yt.get("//sys/@cluster_name")
        job_client_config = deepcopy(yt.config.config)
        job_client_config["backend"] = "rpc"
        job_client_config["cluster_name_for_rpc_proxy_in_job_proxy"] = cluster_name
        job_client_config["enable_rpc_proxy_in_job_proxy"] = True

        def foo():
            client = yt.YtClient(config=job_client_config)
            client.write_table("//tmp/some_table_from_operation", [{"foo": "bar"}])
            sys.stderr.write(f"{client.config['backend']=}")

        vanilla_spec = yt.spec_builders.VanillaSpecBuilder() \
            .begin_task("foo") \
                .spec({"enable_rpc_proxy_in_job_proxy": True}) \
                .environment({"YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB": "1", "YT_FORBID_REQUESTS_FROM_JOB": "0", "YT_LOG_LEVEL": "DEBUG"}) \
                .command(foo) \
                .job_count(1) \
            .end_task()  # noqa

        op = yt.run_operation(vanilla_spec)

        job_infos = op.get_jobs_with_error_or_stderr()
        assert len(job_infos) == 1
        assert "client.config[\'backend\']=\'rpc\'" in job_infos[0]["stderr"]
        assert "Switching to local rpc proxy mode" in job_infos[0]["stderr"]
        assert "Connecting to server (Address: unix:///" in job_infos[0]["stderr"]
        assert list(yt.read_table("//tmp/some_table_from_operation")) == [{"foo": "bar"}]
