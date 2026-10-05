from yt_commands import create, authors, get, set, wait_for_sys_config_sync, write_table

from base import ClickHouseTestBase, Clique
from helpers import get_disabled_cache_config

from yt.common import wait


class TestChunkSpecCache(ClickHouseTestBase):
    def _create_table(self, path="//tmp/t"):
        create(
            "table",
            path,
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            },
        )

    def _get_counters(self, clique):
        profiler = clique.get_profiler()
        hit_counter = profiler.counter("clickhouse/yt/chunk_specs_cache/hit_count", tags={"hit_type": "sync"})
        miss_counter = profiler.counter("clickhouse/yt/chunk_specs_cache/missed_count")
        return hit_counter, miss_counter

    @authors("iharbychyk")
    def test_chunk_spec_cache_hit_on_repeated_query(self):
        self._create_table()
        rows = [{"key": i, "value": "v" + str(i)} for i in range(5)]
        write_table("//tmp/t", rows)

        config_patch = {
            "yt": {
                "subquery": {
                    "chunk_spec_cache": {},
                },
            },
        }
        with Clique(1, config_patch=config_patch) as clique:
            hit_counter, miss_counter = self._get_counters(clique)

            query = 'select * from "//tmp/t" order by key'

            result = clique.make_query(query)
            assert result == rows
            wait(
                lambda: miss_counter.get_delta() > 0,
                error_message="Chunk spec cache did not register a miss on the first query "
                              "(expected missed_count delta > 0, got {})".format(miss_counter.get_delta()),
            )
            assert hit_counter.get_delta() == 0
            miss_delta_after_first_query = miss_counter.get_delta()

            result = clique.make_query(query)
            assert result == rows
            wait(lambda: hit_counter.get_delta() == 1,)
            assert miss_counter.get_delta() == miss_delta_after_first_query

            result = clique.make_query(query)
            assert result == rows
            wait(lambda: hit_counter.get_delta() == 2,)
            assert miss_counter.get_delta() == miss_delta_after_first_query

    @authors("iharbychyk")
    def test_chunk_spec_cache_invalidated_on_table_change(self):
        self._create_table()
        rows = [{"key": i, "value": "v" + str(i)} for i in range(5)]
        write_table("//tmp/t", rows)

        config_patch = {
            "yt": {
                "subquery": {
                    "chunk_spec_cache": {},
                },
                **get_disabled_cache_config()["yt"],
            },
        }
        with Clique(1, config_patch=config_patch) as clique:
            hit_counter, miss_counter = self._get_counters(clique)

            query = 'select * from "//tmp/t" order by key'

            result = clique.make_query(query)
            assert result == rows
            wait(lambda: miss_counter.get_delta() > 0,)
            miss_delta_after_first_query = miss_counter.get_delta()

            new_rows = [{"key": i, "value": "v" + str(i)} for i in range(5, 10)]
            write_table("<append=%true>//tmp/t", new_rows)
            rows += new_rows

            result = clique.make_query(query)
            assert result == rows
            wait(lambda: miss_counter.get_delta() > miss_delta_after_first_query,)
            miss_delta_after_invalidation = miss_counter.get_delta()
            hit_delta_after_invalidation = hit_counter.get_delta()

            result = clique.make_query(query)
            assert result == rows
            wait(lambda: hit_counter.get_delta() > hit_delta_after_invalidation,)
            assert miss_counter.get_delta() == miss_delta_after_invalidation

    @authors("iharbychyk")
    def test_chunk_spec_cache_invalidated_on_chunk_merge(self):
        self._create_table()
        rows = [{"key": i, "value": "v" + str(i)} for i in range(5)]
        for row in rows:
            write_table("<append=%true>//tmp/t", [row])

        account = get("//tmp/t/@account")
        set("//sys/accounts/{}/@merge_job_rate_limit".format(account), 10)
        set("//sys/accounts/{}/@chunk_merger_node_traversal_concurrency".format(account), 1)

        set("//sys/@config/chunk_manager/chunk_merger/enable", True)
        wait_for_sys_config_sync()

        config_patch = {
            "yt": {
                "subquery": {
                    "chunk_spec_cache": {},
                },
                **get_disabled_cache_config()["yt"],
            },
        }
        with Clique(1, config_patch=config_patch) as clique:
            hit_counter, miss_counter = self._get_counters(clique)

            query = 'select * from "//tmp/t" order by key'

            result = clique.make_query(query)
            assert result == rows
            wait(lambda: miss_counter.get_delta() > 0,)
            miss_delta_after_first_query = miss_counter.get_delta()

            chunk_ids_before_merge = get("//tmp/t/@chunk_ids")
            set("//tmp/t/@chunk_merger_mode", "deep")
            wait(lambda: get("//tmp/t/@chunk_ids") != chunk_ids_before_merge,)
            wait(lambda: get("//tmp/t/@chunk_merger_info")["revision"] > 0,)

            result = clique.make_query(query)
            assert result == rows
            wait(lambda: miss_counter.get_delta() > miss_delta_after_first_query,)
