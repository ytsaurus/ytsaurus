from .conftest import (
    Cli,
    SandboxBase,
    generate_uuid,
    prepare_yp_sandbox,
    yatest_save_sandbox,
)

from yp.client import YpClient
from yp.common import GrpcUnavailableError

import contextlib
import os
import pytest
import re
import sys


class YpLocalCli(Cli):
    def __init__(self):
        super(YpLocalCli, self).__init__("yp/python/yp/bin/yp_local_make/yp-local")


@contextlib.contextmanager
def start_yp_local():
    cli = YpLocalCli()

    sandbox_base = SandboxBase()
    sandbox_path = prepare_yp_sandbox(sandbox_base)

    sandbox_name = os.path.basename(sandbox_path)
    os.chdir(os.path.dirname(sandbox_path))

    try:
        cli_stderr_file_path = os.path.join(sandbox_path, "yp_local_stderr_" + generate_uuid())
        with open(cli_stderr_file_path, "w") as cli_stderr_file:
            cli.check_call(
                ["start", "--id", sandbox_name, "--port-locks-path", sandbox_base.get_port_locks_path()],
                stdout=sys.stdout,
                stderr=cli_stderr_file
            )

        cli_stderr = open(cli_stderr_file_path).read().strip()

        sys.stderr.write(cli_stderr)

        matches = re.findall("Grpc address: (localhost:[0-9]+)", cli_stderr)
        assert len(matches) == 1

        yield dict(yp_master_grpc_address=matches[0])
    finally:
        try:
            cli.check_output(["stop", sandbox_name])
        finally:
            yatest_save_sandbox(sandbox_path)


class TestLocalCli(object):
    def test(self):
        yp_client = None
        with start_yp_local() as yp_local:
            yp_client = YpClient(
                address=yp_local["yp_master_grpc_address"],
                transport="grpc",
                config=dict(enable_ssl=False),
            )

            yp_client.create_object("pod_set", attributes=dict(meta=dict(id="podsetid")))
            yp_client.create_object(
                "pod",
                attributes=dict(
                    meta=dict(
                        id="podid",
                        pod_set_id="podsetid",
                    )
                )
            )

            responses = yp_client.select_objects("pod", selectors=["/meta/id"])
            pod_ids = set(map(lambda response: response[0], responses))
            assert "podid" in pod_ids

        assert yp_client is not None
        with pytest.raises(GrpcUnavailableError):
            yp_client.select_objects("pod", selectors=["/meta/id"])
