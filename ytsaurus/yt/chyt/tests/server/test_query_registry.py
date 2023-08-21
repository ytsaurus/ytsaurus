from yt_commands import (create, authors, print_debug, write_table)

from base import ClickHouseTestBase, Clique

from yt.common import YtError, wait

import yt.packages.requests as requests

import pytest
import pprint


class TestQueryRegistry(ClickHouseTestBase):
    @authors("max42")
    def test_query_registry(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 0}])
        with Clique(1, config_patch={"yt": {"process_list_snapshot_update_period": 100}}) as clique:
            monitoring_port = clique.get_active_instances()[0].attributes["monitoring_port"]

            def check_query_registry():
                query_registry = requests.get("http://localhost:{}/orchid/queries".format(monitoring_port)).json()
                running_queries = list(query_registry["running_queries"].values())
                print_debug(running_queries)
                if len(running_queries) < 2:
                    return False
                assert len(running_queries) == 2
                qi = running_queries[0]
                qs = running_queries[1]
                if qi["query_kind"] != "initial_query":
                    qi, qs = qs, qi
                print_debug("Initial: ", pprint.pformat(qi))
                print_debug("Secondary: ", pprint.pformat(qs))
                assert qi["query_kind"] == "initial_query"
                assert qs["query_kind"] == "secondary_query"
                assert "initial_query" in qs
                assert qs["initial_query_id"] == qi["query_id"]
                if qs["query_status"] is None or qi["query_status"] is None:
                    return False
                return True

            t = clique.make_async_query('select sleep(3) from "//tmp/t"')
            wait(lambda: check_query_registry())
            t.join()

    @authors("max42")
    def test_codicils(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        write_table("//tmp/t", [{"a": "foo"}])

        # Normal footprint at start of clique should around 0.5 GiB - 1.5 GiB.
        oom_allocation_size = 2 * 1024 ** 3

        def assert_in_error(config_patch=None, message=None, allocation_size=0):
            print_debug("Checking config_patch={} for '{}' presence".format(config_patch, message))
            clique = Clique(1, config_patch=config_patch)
            with pytest.raises(YtError):
                clique.__enter__()
                clique.make_query(
                    "select sleep(3) from `//tmp/t` settings chyt.testing.subquery_allocation_size = {}".format(
                        allocation_size
                    ),
                    verbose=False,
                )
            clique.op.track(False, False)
            assert message in str(clique.op.get_error())
            with pytest.raises(YtError):
                clique.__exit__(None, None, None)

        def assert_no_error(config_patch=None, completed_job_count=0, allocation_size=0):
            print_debug("Checking config_patch={} for no error".format(config_patch))
            with Clique(1, config_patch=config_patch) as clique:
                assert clique.make_query(
                    "select sleep(3) from `//tmp/t` settings chyt.testing.subquery_allocation_size = {}".format(
                        allocation_size
                    ),
                    verbose=False,
                ) == [{"sleep(3)": 0}]
                # NB: interrupted jobs are considered to be lost.
                wait(lambda: clique.op.get_job_count("lost") >= completed_job_count)

        assert_no_error()
        assert_in_error(
            config_patch={
                "yt": {
                    "memory_watchdog": {
                        "memory_limit": 10 * 1024 ** 3,
                        "period": 50,
                        "codicil_watermark": 8 * 1024 ** 3,
                    }
                }
            },
            message="because memory usage",
            allocation_size=oom_allocation_size,
        )
        assert_no_error(
            config_patch={
                "yt": {
                    "memory_watchdog": {
                        "memory_limit": 10 * 1024 ** 3,
                        "period": 50,
                        "window_codicil_watermark": 8 * 1024 ** 3,
                        "window_width": 200,
                    }
                }
            },
            allocation_size=oom_allocation_size,
            completed_job_count=1,
        )

    @authors("max42")
    @pytest.mark.skipif(True, reason="temporarily broken")
    def test_datalens_header(self):
        with Clique(1) as clique:
            t = clique.make_async_query_via_proxy("select sleep(3)", headers={"X-Request-Id": "ummagumma"})
            wait(
                lambda: "ummagumma"
                        in str(clique.get_orchid(clique.get_active_instances()[0], "/queries/running_queries")),
                iter=10,
            )
            t.join()
