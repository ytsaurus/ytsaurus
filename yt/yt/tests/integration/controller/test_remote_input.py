from yt_env_setup import YTEnvSetup

from yt_commands import authors, get_driver, merge, write_table, read_table, create, start_transaction

from yt.environment.helpers import assert_items_equal

import yt.yson as yson


@authors("a-romanov")
class TestInputFromRemote(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1
    NUM_REMOTE_CLUSTERS = 1
    NUM_TEST_PARTITIONS = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "remote_operations": {
                "remote_0": {"allowed_for_everyone": True}
            }
        }
    }

    def test_merge(self):
        attributes = {"schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]}
        create("table", "//tmp/t0", attributes=attributes)
        create("table", "//tmp/t0", attributes=attributes, driver=get_driver(cluster="remote_0"))

        rows = [{"a": 42, "b": "foo"}, {"a": -17, "b": "bar"}]
        write_table("//tmp/t0", rows, driver=get_driver(cluster="remote_0"))

        merge(in_=yson.loads('[<cluster="remote_0">"//tmp/t0"]'.encode('utf8')), out="//tmp/t0")

        assert_items_equal(read_table("//tmp/t0"), rows)

    def test_merge_with_tx(self):
        attributes = {"schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]}
        create("table", "//tmp/t1", attributes=attributes)
        create("table", "//tmp/t1", attributes=attributes, driver=get_driver(cluster="remote_0"))

        rows = [{"a": 42, "b": "foo"}, {"a": -17, "b": "bar"}]
        write_table("//tmp/t1", rows, driver=get_driver(cluster="remote_0"))

        tx = start_transaction(timeout=100000, driver=get_driver(cluster="remote_0"))

        merge(in_=yson.loads('[<cluster="remote_0";transaction_id="{}">"//tmp/t1"]'.format(tx).encode('utf8')), out="//tmp/t1")

        assert_items_equal(read_table("//tmp/t1"), rows)
