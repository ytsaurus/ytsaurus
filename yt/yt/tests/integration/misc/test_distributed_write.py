from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors,
    create_table, read_table,
    start_distributed_write_session, finish_distributed_write_session, write_table_fragment,
    raises_yt_error
)

from yt.common import YtResponseError

import yt.yson as yson

import pytest
import time


@pytest.mark.enabled_multidaemon
class TestDistributedWrite(YTEnvSetup):
    TABLE_PATH = "//tmp/distributed_table_test"
    TABLE_SCHEMA = [{"name": "v1", "type": "int64", "sort_order": "ascending"}]

    @authors("pavook")
    def test_end_to_end(self):
        create_table(self.TABLE_PATH, schema=self.TABLE_SCHEMA)

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

    @authors("achains")
    def test_session_timeout(self):
        create_table(self.TABLE_PATH, schema=self.TABLE_SCHEMA)

        options = {
            "session_timeout": 1,
        }

        session_with_cookies = start_distributed_write_session(self.TABLE_PATH, cookie_count=1, **options)
        time.sleep(2)
        with pytest.raises(YtResponseError):
            try:
                finish_distributed_write_session(session_with_cookies["session"], [])
            except YtResponseError as e:
                assert "No such transaction" in str(e)
                raise e


@pytest.mark.enabled_multidaemon
class TestDistributedWriteRpcProxy(TestDistributedWrite):
    ENABLE_RPC_PROXY = True
    DELTA_RPC_DRIVER_CONFIG = {
        "enable_retries": True,
    }
    DELTA_RPC_PROXY_CONFIG = {
        "signature_components": {
            "validation": {
                "cypress_key_reader": dict(),
            },
            "generation": {
                "cypress_key_writer": dict(),
                "key_rotator": dict(),
                "generator": dict(),
            },
        },
    }
    DRIVER_BACKEND = "rpc"
    PWN_PATH = "//tmp/pwned"

    @authors("pavook")
    def test_cookie_modification(self):
        create_table(self.TABLE_PATH, schema=self.TABLE_SCHEMA)

        session_with_cookies = start_distributed_write_session(self.TABLE_PATH, cookie_count=0)
        session = session_with_cookies["session"]
        pwned_session = yson.loads(yson.dumps(session).replace(
            bytes(self.TABLE_PATH[:len(self.PWN_PATH)], "utf-8"),
            bytes(self.PWN_PATH, "utf-8")))
        with raises_yt_error("Signature validation failed"):
            finish_distributed_write_session(pwned_session, [])
