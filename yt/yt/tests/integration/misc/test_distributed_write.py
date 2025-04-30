from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors,
    create_table, read_table,
    start_distributed_write_session, finish_distributed_write_session, write_table_fragment,
    raises_yt_error,
)

import yt.yson as yson

import pytest


@pytest.mark.enabled_multidaemon
class TestDistributedWrite(YTEnvSetup):
    TABLE_PATH = "//tmp/distributed_table_test"

    @authors("pavook")
    def test_end_to_end(self):
        create_table(self.TABLE_PATH, schema=[{"name": "v1", "type": "int64", "sort_order": "ascending"}])

        num_rows = 10
        num_cookies = 10
        rows = [{"v1": i} for i in range(num_rows * num_cookies)]

        session_with_cookies = start_distributed_write_session(self.TABLE_PATH, cookie_count=num_cookies)
        session, cookies = (session_with_cookies["session"], session_with_cookies["cookies"])
        results = []
        for i in range(num_cookies):
            results.append(write_table_fragment(cookies[i], rows[i * num_rows : (i + 1) * num_rows]))
        finish_distributed_write_session(session, results)

        assert read_table(self.TABLE_PATH) == rows


@pytest.mark.enabled_multidaemon
class TestDistributedWriteRPC(TestDistributedWrite):
    ENABLE_RPC_PROXY = True
    DELTA_RPC_PROXY_CONFIG = {
        "signature_validation": {
            "cypress_key_reader": dict(),
            "validator": dict(),
        },
        "signature_generation": {
            "cypress_key_writer": {
                "owner_id": "test-rpc-proxy",
            },
            "key_rotator": {
                "key_rotation_interval": "2h",
            },
            "generator": dict(),
        },
    }
    DRIVER_BACKEND = "rpc"
    PWN_PATH = "//tmp/pwned"

    @authors("pavook")
    def test_cookie_modification(self):
        create_table(self.TABLE_PATH, schema=[{"name": "v1", "type": "int64", "sort_order": "ascending"}])

        session_with_cookies = start_distributed_write_session(self.TABLE_PATH, cookie_count=0)
        session = session_with_cookies["session"]
        pwned_session = yson.loads(yson.dumps(session).replace(
            bytes(self.TABLE_PATH[:len(self.PWN_PATH)], "utf-8"),
            bytes(self.PWN_PATH, "utf-8")))
        with raises_yt_error("Signature validation failed"):
            finish_distributed_write_session(pwned_session, [])
