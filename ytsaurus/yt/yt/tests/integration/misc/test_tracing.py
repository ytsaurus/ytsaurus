from yt_env_setup import YTEnvSetup

from yt_commands import authors, sync_create_cells, map, create, write_table

import yt.wrapper

import time


##################################################################


class TestTracing(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True
    USE_DYNAMIC_TABLES = True

    DELTA_PROXY_CONFIG = {
        "api": {
            "force_tracing": True,
        },
    }

    DELTA_RPC_PROXY_CONFIG = {
        "api_service": {
            "force_tracing": True,
        },
    }

    @classmethod
    def teardown_class(cls):
        # Wait for tracing queue flush
        time.sleep(10)

        super(TestTracing, cls).teardown_class()

    @authors("prime")
    def test_tracing_http_cypress(self):
        yw = yt.wrapper.YtClient(proxy=self.Env.get_proxy_address())
        yw.get("//@")
        yw.create("map_node", "//home/prime", force=True, recursive=True)
        yw.create("table", "//home/prime/t")

        yw.write_table("//home/prime/t", [{"foo": "bar"}])
        assert list(yw.read_table("//home/prime/t")) == [{"foo": "bar"}]

    @authors("prime")
    def test_tracing_http_scheduler(self):
        yw = yt.wrapper.YtClient(proxy=self.Env.get_proxy_address())
        yw.create("map_node", "//home/prime", force=True, recursive=True)

        yw.create("table", "//tmp/t")
        yw.create("table", "//tmp/out")
        yw.write_table("//home/prime/t", [{"foo": "bar"}])

        yw.run_map("cat", "//tmp/t", "//tmp/out", format="yson", sync=True)

    @authors("prime")
    def test_tracing_http_dynamic_tables(self):
        sync_create_cells(4)

        yw = yt.wrapper.YtClient(proxy=self.Env.get_proxy_address())

        yw.create("table", "//tmp/d", attributes={
            "dynamic": True,
            "external": True,
            "schema": [
                {"name": "key", "type": "string", "sort_order": "descending"},
                {"name": "value", "type": "int64"},
            ],
        })

        yw.reshard_table("//tmp/d", pivot_keys=[[], ["a"], ["b"], ["c"]], sync=True)
        yw.mount_table("//tmp/d", sync=True)

        yw.insert_rows("//tmp/d", [
            {"key": "a", "value": 1},
            {"key": "b", "value": 2},
            {"key": "с", "value": 3},
            {"key": "d", "value": 4},
        ])

        yw.insert_rows("//tmp/d", [
            {"key": "a", "value": 1},
        ])

        yw.lookup_rows("//tmp/d", [
            {"key": "a"},
            {"key": "b"},
            {"key": "с"},
            {"key": "d"},
        ])

        yw.select_rows("sum(value) from [//tmp/d] group by 1")

        yw.select_rows("sum(value) from [//tmp/d] group by key")

    @authors("prime")
    def test_job_proxy_tracing(self):
        create("table", "//tmp/t")
        create("table", "//tmp/out")
        write_table("//tmp/t", [{"foo": "bar"}])

        map(command="sleep 5; cat", in_="//tmp/t", out="//tmp/out", format="yson", spec={
            "force_job_proxy_tracing": True,
        })
