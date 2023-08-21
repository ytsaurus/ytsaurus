from base import ClickHouseTestBase, Clique

from yt_commands import (write_table, authors, print_debug, create, merge)

import os
import pytest
import time


def is_tracing_enabled():
    return "YT_TRACE_DUMP_DIR" in os.environ


class TestTracing(ClickHouseTestBase):
    @authors("max42")
    @pytest.mark.parametrize("trace_method", ["x-yt-sampled", "traceparent", "chyt.enable_tracing"])
    def test_tracing_via_http_proxy(self, trace_method):
        with Clique(1) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
            for i in range(5):
                write_table("<append=%true>//tmp/t", [{"a": 2 * i}, {"a": 2 * i + 1}])

            settings = None
            headers = {}
            if trace_method == "x-yt-sampled":
                headers["X-Yt-Sampled"] = "1"
            elif trace_method == "traceparent":
                headers["traceparent"] = "00-11111111222222223333333344444444-5555555566666666-01"
            else:
                settings = {"chyt.enable_tracing": 1}

            result = clique.make_query_via_proxy(
                'select avg(a) from "//tmp/t"',
                headers=headers,
                full_response=True,
                settings=settings,
            )
            assert abs(result.json()["data"][0]["avg(a)"] - 4.5) < 1e-6
            query_id = result.headers["X-ClickHouse-Query-Id"]
            print_debug("Query id =", query_id)

            if trace_method == "traceparent":
                assert query_id.startswith("33333333-44444444")

            if is_tracing_enabled():
                # TODO(max42): this seems broken after moving to Porto.

                # Check presence of one of the middle parts of query id in the binary trace file.
                # It looks like a good evidence of that everything works fine. Don't tell prime@ that
                # I rely on tracing binary protobuf representation, though :)
                query_id_part = query_id.split("-")[2].rjust(8, "0")
                query_id_part_binary = "".join(
                    chr(int(a, 16) * 16 + int(b, 16)) for a, b in reversed(zip(query_id_part[::2], query_id_part[1::2]))
                )

                time.sleep(2)

                pid = clique.make_query("select pid from system.clique")[0]["pid"]
                tracing_file = open(os.path.join(os.environ["YT_TRACE_DUMP_DIR"], "ytserver-clickhouse." + str(pid)))
                content = tracing_file.read()
                assert query_id_part_binary in content
            else:
                pytest.skip("Rest of this test is not working because YT_TRACE_DUMP_DIR is not in env")

    @authors("max42")
    @pytest.mark.skipif(not is_tracing_enabled(), reason="YT_TRACE_DUMP_DIR should be in env")
    def test_large_tracing(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        write_table("//tmp/t", [{"a": "a"}])
        merge(in_=["//tmp/t"] * 100, out="//tmp/t")

        with Clique(5) as clique:
            assert clique.make_query('select count(*) from "//tmp/t"')[0]["count()"] == 100
