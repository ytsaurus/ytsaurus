from base import ClickHouseTestBase, Clique

from yt_commands import (authors, print_debug)

from yt.common import wait

import time
import pytest


class TestQueryLog(ClickHouseTestBase):
    @staticmethod
    def _get_engine_specification(period):
        return (
            "ENGINE = Buffer('{{database}}', '{{underlying_table_name}}', 1, 1, {}, "
            "1000000000000, 1000000000000, 1000000000000, 1000000000000)".format(period)
        )

    @authors("max42")
    def test_query_log_simple(self):
        with Clique(
                1, config_patch={"clickhouse": {"query_log": {"engine": self._get_engine_specification(1)}}}
        ) as clique:
            clique.make_query("select 1")
            wait(lambda: len(clique.make_query("select * from system.query_log")) >= 1)
            time.sleep(6)
            assert len(clique.make_query("select * from system.query_log")) == 0

    @authors("max42")
    @pytest.skip('Temporary broken because of CH bug: CHYT-633')
    def test_query_log_eviction(self):
        period = 3

        with Clique(
                1, config_patch={"clickhouse": {"query_log": {"engine": self._get_engine_specification(period)}}}
        ) as clique:
            timespan = 15
            counter = 0
            start = time.time()

            identifier_to_state = dict()

            while True:
                now = time.time() - start
                if now > timespan:
                    break
                identifier = "foo{:03}".format(counter)
                assert clique.make_query("select 1 as {}".format(identifier), verbose=False)[0] == {identifier: 1}
                counter += 1
                identifier_to_state[identifier] = {"state": "unseen", "started_at": now}

                all_entries = clique.make_query("select * from system.query_log", verbose=False)
                seen_now = {None}
                for entry in all_entries:
                    if not entry["query"]:
                        continue
                    foo_index = entry["query"].find("foo")
                    if foo_index != -1:
                        found_identifier = entry["query"][foo_index:foo_index + 6]
                        seen_now.add(found_identifier)
                        state = identifier_to_state[found_identifier]
                        if state["state"] == "unseen":
                            state["state"] = "seen"
                            state["seen_at"] = now
                for identifier, state in identifier_to_state.items():
                    if identifier in seen_now:
                        continue
                    if state["state"] == "seen":
                        state["state"] = "evicted"
                        state["evicted_at"] = now

        for identifier, state in sorted(identifier_to_state.items(), key=lambda pair: pair[0]):
            print_debug(identifier, state)
            if state["state"] in ("seen", "evicted"):
                assert state["seen_at"] - state["started_at"] <= 0.5
            if state["state"] == "evicted":
                assert state["evicted_at"] - state["started_at"] >= period * 0.5
                assert state["evicted_at"] - state["started_at"] <= period * 3.5

            if state["started_at"] <= 1.5 * period:
                assert state["state"] == "evicted"
            if state["started_at"] <= 3.5 * period:
                assert state["state"] in ("seen", "evicted")
