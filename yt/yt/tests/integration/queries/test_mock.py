from yt_env_setup import YTEnvSetup

from yt_commands import (
    add_member, authors, create_access_control_object, remove,
    make_ace, raises_yt_error, wait, create_user, print_debug, select_rows,
    set, get, insert_rows, sync_compact_table, generate_uuid, ls)

from yt_error_codes import AuthorizationErrorCode, ResolveErrorCode

from yt_helpers import profiler_factory

from yt_queries import Query, start_query, list_queries, get_query_tracker_info, get_query

from yt.common import date_string_to_timestamp_mcs

from yt.test_helpers import assert_items_equal

from yt.wrapper import yson

from collections import Counter
from builtins import set as Set

import pytest
import time


def expect_queries(queries, list_result, incomplete=False):
    try:
        assert Set(q.id for q in queries) == Set(q["id"] for q in list_result["queries"])
        assert list_result["incomplete"] == incomplete
    except AssertionError:
        timestamp = list_result["timestamp"]
        print_debug(f"Assertion failed, dumping content of dynamic tables by timestamp {timestamp}")
        for table in ("active_queries", "finished_queries", "finished_queries_by_start_time", "finished_queries_by_aco_and_start_time", "finished_queries_by_user_and_start_time"):
            print_debug(f"{table}:")
            select_rows(f"* from [//sys/query_tracker/{table}]", timestamp=timestamp)
        raise


@pytest.mark.enabled_multidaemon
class TestMetrics(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @authors("mpereskokova")
    def test_active_queries_metrics(self, query_tracker):
        query_tracker = ls("//sys/query_tracker/instances")[0]
        profiler = profiler_factory().at_query_tracker(query_tracker)

        q = start_query("mock", "run_forever")

        active_queries_metric = profiler.gauge("query_tracker/active_queries")
        wait(lambda: active_queries_metric.get({"state": "Running"}) == 1)

        q.abort()
        wait(lambda: active_queries_metric.get({"state": "Running"}) == 0)

    @authors("mpereskokova")
    def test_state_time_metrics(self, query_tracker):
        query_tracker = ls("//sys/query_tracker/instances")[0]
        profiler = profiler_factory().at_query_tracker(query_tracker)
        state_time_metric = profiler.gauge("query_tracker/state_time")

        q1 = start_query("mock", "run_forever")
        wait(lambda: state_time_metric.get({"state": "Pending"}) is not None)
        assert state_time_metric.get({"state": "Running"}) is None
        assert state_time_metric.get({"state": "Aborting"}) is None

        q1.abort()
        wait(lambda: state_time_metric.get({"state": "Running"}) is not None)
        wait(lambda: state_time_metric.get({"state": "Aborting"}) is not None)
        assert state_time_metric.get({"state": "Failing"}) is None

        q2 = start_query("mock", "fail")
        with raises_yt_error("failed"):
            q2.track()
        wait(lambda: state_time_metric.get({"state": "Failing"}) is not None)
        assert state_time_metric.get({"state": "Completing"}) is None

        q3 = start_query("mock", "complete_after", settings={"duration": 0})
        q3.track()
        wait(lambda: state_time_metric.get({"state": "Completing"}) is not None)


@pytest.mark.enabled_multidaemon
class TestQueriesMock(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }
    ENABLE_MULTIDAEMON = True

    @authors("max42")
    def test_fail(self, query_tracker):
        q = start_query("mock", "fail")
        with raises_yt_error("failed"):
            q.track()
        assert q.get_state() == "failed"

        q = start_query("mock", "fail_by_exception")
        with raises_yt_error("failed"):
            q.track()
        assert q.get_state() == "failed"

        q = start_query("mock", "fail_after", settings={"duration": 3000})
        with raises_yt_error("failed"):
            q.track()
        assert q.get_state() == "failed"

    @authors("max42")
    def test_abort(self, query_tracker):
        q = start_query("mock", "run_forever")
        wait(lambda: q.get_state() == "running")
        q.abort()
        with raises_yt_error("aborted"):
            q.track()
        assert q.get_state() == "aborted"

    @authors("max42")
    def test_complete(self, query_tracker):
        error = {"code": 42, "message": "Mock query execution error", "attributes": {"some_attr": "some_value"}}
        schema = [{"name": "foo", "type": "int64"}, {"name": "bar", "type": "string"}]
        rows = [{"foo": 42, "bar": "abc"}, {"foo": -17, "bar": "def"}, {"foo": 123, "bar": "ghi"}]

        q = start_query("mock", "complete_after", settings={
            "duration": 3000,
            "results": [
                {"error": error},
                {"schema": schema, "rows": rows},
            ]
        })

        q.track()
        query_info = q.get()
        assert query_info["result_count"] == 2

        result_0_info = q.get_result(0)
        assert result_0_info["error"] == error
        with raises_yt_error("Mock query execution error"):
            q.read_result(0)

        result_1_info = q.get_result(1)
        for column in result_1_info["schema"]:
            del column["type_v3"]
            del column["required"]
        result_1_info["schema"] = list(result_1_info["schema"])
        assert result_1_info["data_statistics"]["row_count"] == 3
        assert result_1_info["data_statistics"]["data_weight"] == 36
        assert result_1_info["schema"] == schema
        assert_items_equal(q.read_result(1), rows)
        assert_items_equal(q.read_result(1, lower_row_index=1, upper_row_index=2), rows[1:2])
        assert_items_equal(q.read_result(1, lower_row_index=-1, upper_row_index=5), rows)
        assert_items_equal(q.read_result(1, lower_row_index=2, upper_row_index=1), [])
        assert_items_equal(q.read_result(1, columns=["foo"]), [{"foo": row["foo"]} for row in rows])
        assert q.read_result(1, columns=["bar", "foo", "bar"], output_format="dsv") == \
            b"""bar=abc\tfoo=42\nbar=def\tfoo=-17\nbar=ghi\tfoo=123\n"""

    @authors("kirsiv40")
    def test_assigned_tracker_attribute_saves_after_query_finishes(self, query_tracker):
        error = {"code": 42, "message": "Mock query execution error", "attributes": {"some_attr": "some_value"}}
        schema = [{"name": "foo", "type": "int64"}, {"name": "bar", "type": "string"}]
        rows = [{"foo": 42, "bar": "abc"}, {"foo": -17, "bar": "def"}, {"foo": 123, "bar": "ghi"}]

        q = start_query("mock", "complete_after", settings={
            "duration": 3000,
            "results": [
                {"error": error},
                {"schema": schema, "rows": rows},
            ]
        })

        wait(lambda: q.get_state() == "running")

        active_query_info = q.get()
        assert "assigned_tracker" in active_query_info

        q.track()

        finished_query_info = q.get()
        assert "assigned_tracker" in finished_query_info
        assert finished_query_info["assigned_tracker"] == active_query_info["assigned_tracker"]

    @authors("max42", "kirsiv40")
    def test_list(self, query_tracker):
        create_user("u1")
        create_user("u2")
        q0 = start_query("mock", "fail", authenticated_user="u1")
        q1 = start_query("mock", "complete_after", settings={"duration": 16000}, authenticated_user="u2")
        q2 = start_query("mock", "fail_by_exception", authenticated_user="u1")
        q3 = start_query("mock", "blahblah", authenticated_user="u2")
        q4 = start_query("mock", "fail_after", settings={"duration": 8000}, authenticated_user="u2")
        q5 = start_query("mock", "run_forever", authenticated_user="u1")
        q6 = start_query("mock", "run_forever", authenticated_user="u1")

        def collect_batch(attribute):
            queries = list_queries(cursor_direction="future", attributes=["id", attribute])
            result = []
            for q, expected_q in zip(queries["queries"], (q0, q1, q2, q3, q4, q5, q6)):
                assert q["id"] == expected_q.id
                result.append(q[attribute])
            return result

        q_times = collect_batch("start_time")

        wait(lambda: q6.get_state() == "running")
        q6.abort()

        def run_checks():
            expect_queries([q6, q5, q4, q3, q2, q1, q0], list_queries())
            expect_queries([q0, q1, q2, q3, q4, q5, q6], list_queries(cursor_direction="future"))

            expect_queries([q6, q5, q2, q0], list_queries(user="u1"))
            expect_queries([q4, q3, q1], list_queries(user="u2"))

            expect_queries([q5, q4],
                           list_queries(from_time=q_times[1], to_time=q_times[5], limit=2),
                           incomplete=True)
            expect_queries([q3, q2],
                           list_queries(from_time=q_times[1], to_time=q_times[5], limit=2, cursor_time=q_times[4]),
                           incomplete=True)
            expect_queries([q1],
                           list_queries(from_time=q_times[1], to_time=q_times[5], limit=2, cursor_time=q_times[2]))
            expect_queries([q1, q2, q3],
                           list_queries(from_time=q_times[1], to_time=q_times[5], limit=3, cursor_direction="future"),
                           incomplete=True)
            expect_queries([q4, q5],
                           list_queries(from_time=q_times[1], to_time=q_times[5], limit=3, cursor_direction="future",
                                        cursor_time=q_times[3]))

        while True:
            run_checks()
            q_states = collect_batch("state")
            print_debug("Query states:", q_states, "counts:", Counter(q_states))
            if q_states == ["failed", "completed", "failed", "failed", "failed", "running", "aborted"]:
                break

        expect_queries([q6], list_queries(state="aborted"))
        expect_queries([q4, q3, q2, q0], list_queries(state="failed"))
        expect_queries([q5], list_queries(state="running"))
        expect_queries([q1], list_queries(state="completed"))

        def check_sort_order(future_expected_asc, past_expected_asc, incomplete=False, **list_queries_kwargs):
            expect_queries(future_expected_asc, list_queries(cursor_direction="future", sort_order="ascending", **list_queries_kwargs), incomplete=incomplete)
            expect_queries(future_expected_asc[::-1], list_queries(cursor_direction="future", sort_order="descending", **list_queries_kwargs), incomplete=incomplete)
            expect_queries(future_expected_asc[::-1], list_queries(cursor_direction="future", sort_order="cursor", **list_queries_kwargs), incomplete=incomplete)
            expect_queries(future_expected_asc[::-1], list_queries(cursor_direction="future", **list_queries_kwargs), incomplete=incomplete)

            expect_queries(past_expected_asc, list_queries(cursor_direction="past", sort_order="ascending", **list_queries_kwargs), incomplete=incomplete)
            expect_queries(past_expected_asc[::-1], list_queries(cursor_direction="past", sort_order="descending", **list_queries_kwargs), incomplete=incomplete)
            expect_queries(past_expected_asc, list_queries(cursor_direction="past", sort_order="cursor", **list_queries_kwargs), incomplete=incomplete)
            expect_queries(past_expected_asc, list_queries(cursor_direction="past", **list_queries_kwargs), incomplete=incomplete)

        check_sort_order([q0, q1, q2, q3, q4, q5, q6], [q0, q1, q2, q3, q4, q5, q6], incomplete=False)
        check_sort_order([q0, q1], [q5, q6], incomplete=True, limit=2)
        check_sort_order([q4, q5, q6], [q0, q1, q2], incomplete=False, cursor_time=q_times[3])
        check_sort_order([q4, q5], [q1, q2], incomplete=True, limit=2, cursor_time=q_times[3])

    @authors("max42")
    def test_draft(self, query_tracker):
        q = start_query("mock", "blahblah", draft=True)
        q_info = q.get()
        assert q_info["state"] == "draft"
        assert q_info["query"] == "blahblah"
        assert list_queries()["queries"] == [q_info]

    @authors("max42")
    def test_annotations(self, query_tracker):
        q = start_query("mock", "complete_after", settings={"duration": 5000}, annotations={"foo": "bar"})
        wait(lambda: q.get_state() == "running")
        assert len(list_queries(filter="bar", use_full_text_search=False)["queries"]) == 1
        assert len(list_queries(filter="bar", use_full_text_search=True)["queries"]) == 1

        q.alter(annotations={"qux": "quux"})
        q_info = list_queries(filter="qux", attributes=["annotations", "state"], use_full_text_search=True)["queries"][0]
        assert q_info["annotations"] == {"qux": "quux"}
        assert q_info["state"] == "running"
        q_info = list_queries(filter="qux", attributes=["annotations", "state"], use_full_text_search=False)["queries"][0]
        assert q_info["annotations"] == {"qux": "quux"}
        assert q_info["state"] == "running"
        wait(lambda: q.get_state() == "completed")

        assert len(list_queries(filter="qux", use_full_text_search=False)["queries"]) > 0
        assert len(list_queries(filter="qux", use_full_text_search=True)["queries"]) > 0

        q.alter(annotations={"qwe": "asd"})
        assert len(list_queries(filter="asd", use_full_text_search=False)["queries"]) > 0
        assert len(list_queries(filter="asd", use_full_text_search=True)["queries"]) > 0

    @authors("mpereskokova")
    def test_rows_limit(self, query_tracker):
        schema = [{"name": "a", "type": "int64"}]
        rows = [{"a": 42}, {"a": 43}, {"a": 44}]
        q = start_query("mock", "complete_after", settings={
            "results": [
                {"schema": schema, "rows": rows, "is_truncated": True},
                {"schema": schema, "rows": rows, "is_truncated": False},
            ]
        })

        q.track()
        assert q.get_result(0)["is_truncated"]
        assert not q.get_result(1)["is_truncated"]

    @authors("kirsiv40")
    def test_is_indexed_flag(self, query_tracker):
        q = start_query("mock", "complete_after", settings={"duration": 1000})
        assert len(list_queries()["queries"]) == 1
        assert str(get_query(q.id)["is_indexed"]) == "true"
        q.track()
        assert len(list_queries()["queries"]) == 1
        assert str(get_query(q.id)["is_indexed"]) == "true"
        q = start_query("mock", "complete_after", settings={"duration": 1000, 'is_indexed': False})
        assert len(list_queries()["queries"]) == 1
        assert str(get_query(q.id)["is_indexed"]) == "false"
        q.track()
        assert len(list_queries()["queries"]) == 1
        assert str(get_query(q.id)["is_indexed"]) == "false"


@pytest.mark.enabled_multidaemon
class TestQueryTrackerBan(YTEnvSetup):
    NUM_QUERY_TRACKER = 1
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }
    QUERY_TRACKER_DYNAMIC_CONFIG = {"state_check_period": 2000}
    ENABLE_MULTIDAEMON = True

    def _test_query_fails():
        start_query("mock", "run_forever")
        return False

    @authors("mpereskokova")
    def test_query_tracker_ban(self, query_tracker):
        address = query_tracker.query_tracker.addresses[0]
        set(f"//sys/query_tracker/instances/{address}/@banned", True)

        with raises_yt_error() as err:
            wait(TestQueryTrackerBan._test_query_fails)
        assert err[0].contains_text("No alive peers found")
        wait(lambda: get(f"//sys/query_tracker/instances/{address}/orchid/state_checker/banned"), ignore_exceptions=True)

        guid = generate_uuid()
        insert_rows("//sys/query_tracker/active_queries", [{
            "query_id": guid,
            "engine": "mock",
            "user": "root",
            "query": "run_forever",
            "incarnation": 0,
            "start_time": 0,
            "execution_start_time": 0,
            "state": "pending",
            "settings": {},
            "annotations": {},
        }])

        acquisition_iterations = get(f"//sys/query_tracker/instances/{address}/orchid/query_tracker/acquisition_iterations")
        wait(lambda: get(f"//sys/query_tracker/instances/{address}/orchid/query_tracker/acquisition_iterations") - acquisition_iterations >= 3)

        assert list(select_rows(f'* from [//sys/query_tracker/active_queries] WHERE query_id = "{guid}"'))[0]["state"] == "pending"

        set(f"//sys/query_tracker/instances/{address}/@banned", False)
        query = Query(guid)
        wait(lambda: query.get_state() == "running", ignore_exceptions=True)


@pytest.mark.enabled_multidaemon
class TestQueryTrackerResults(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }
    ENABLE_MULTIDAEMON = True

    @authors("lucius")
    def test_query_tracker_results(self, query_tracker):
        guid = generate_uuid()
        insert_rows("//sys/query_tracker/finished_queries", [{
            "query_id": guid,
            "engine": "mock",
            "query": "select *",
            "files": None,
            "settings": None,
            "user": "test",
            "access_control_objects": None,
            "start_time": 0,
            "state": "completed",
            "progress": "",
            "error": {"attributes": {}, "code": 100, "message": ""},
            "result_count": 1,
            "finish_time": 0,
            "annotations": None,
        }])
        full_result = {"cluster": "test", "table_path": "tmp/test"}
        insert_rows("//sys/query_tracker/finished_query_results", [{
            "query_id": guid,
            "result_index": 0,
            "error": {"attributes": {}, "code": 100, "message": ""},
            "schema": [{"name": "a", "type": "int64"}],
            "data_statistics": {},
            "rowset": """[{"a": 1}]""",
            "is_truncated": False,
            "full_result": full_result,
        }])
        query = Query(guid)
        assert query.get_result(0)["full_result"] == yson.YsonMap(full_result)

    @authors("lucius")
    def test_query_tracker_results_empty(self, query_tracker):
        guid = generate_uuid()
        insert_rows("//sys/query_tracker/finished_queries", [{
            "query_id": guid,
            "engine": "mock",
            "query": "select *",
            "files": None,
            "settings": None,
            "user": "test",
            "access_control_objects": None,
            "start_time": 0,
            "state": "completed",
            "progress": "",
            "error": {"attributes": {}, "code": 100, "message": ""},
            "result_count": 1,
            "finish_time": 0,
            "annotations": None,
        }])
        insert_rows("//sys/query_tracker/finished_query_results", [{
            "query_id": guid,
            "result_index": 0,
            "error": {"attributes": {}, "code": 100, "message": ""},
            "schema": [{"name": "a", "type": "int64"}],
            "data_statistics": {},
            "rowset": """[{"a": 1}]""",
            "is_truncated": False,
            "full_result": None,
        }])
        query = Query(guid)
        assert query.get_result(0)["full_result"] == yson.YsonEntity()


@pytest.mark.enabled_multidaemon
class TestQueryTrackerQueryRestart(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }
    ENABLE_MULTIDAEMON = True

    def _insert_query(self, state, query="run_forever", settings={}, error="", is_abort=False):
        guid = generate_uuid()
        query = {
            "query_id": guid,
            "engine": "mock",
            "user": "root",
            "query": query,
            "incarnation": 0,
            "start_time": 0,
            "execution_start_time": 0,
            "progress": "",
            "annotations": {},
            "state": state,
            "settings": settings,
        }
        if is_abort:
            query["abort_request"] = {"attributes": {}, "code": 100, "message": error}
        else:
            query["error"] = {"attributes": {}, "code": 100, "message": error}

        insert_rows("//sys/query_tracker/active_queries", [query])
        return guid

    @authors("mpereskokova")
    def test_query_tracker_aborting_query(self, query_tracker):
        error = "test_abort"
        guid = self._insert_query("aborting", error, is_abort=True)

        query = Query(guid)
        with raises_yt_error("aborted"):
            query.track()

    @authors("mpereskokova")
    def test_query_tracker_failing_query(self, query_tracker):
        guid = self._insert_query("failing")

        query = Query(guid)
        with raises_yt_error("failed"):
            query.track()

    @authors("mpereskokova")
    def test_query_tracker_completing_query(self, query_tracker):
        guid = self._insert_query("completing")

        query = Query(guid)
        query.track()

    @authors("mpereskokova")
    def test_query_tracker_running_query(self, query_tracker):
        guid = self._insert_query("running", query="complete_after", settings={"duration": 100})

        query = Query(guid)
        query.track()

    @authors("mpereskokova")
    def test_query_tracker_pending_query(self, query_tracker):
        guid = self._insert_query("pending", query="complete_after", settings={"duration": 100})

        query = Query(guid)
        query.track()


@pytest.mark.enabled_multidaemon
class TestAccessControl(YTEnvSetup):
    NUM_TEST_PARTITIONS = 16

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    ENABLE_MULTIDAEMON = True

    @authors("krock21")
    def test_no_acos(self, query_tracker):
        create_user("u1")
        create_user("u2")
        q_u1 = start_query("mock", "u1", authenticated_user="u1")
        q_u2 = start_query("mock", "u2", authenticated_user="u2")
        q_u1.get(authenticated_user="u1")
        q_u2.get(authenticated_user="u2")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.get(authenticated_user="u2")
        with raises_yt_error(AuthorizationErrorCode):
            q_u2.get(authenticated_user="u1")

    @authors("krock21")
    def test_aco_does_not_exist(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("superuser_u3")
        add_member("superuser_u3", "superusers")
        create_access_control_object(
            "aco1",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "use"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco1")
        with raises_yt_error(ResolveErrorCode):
            start_query("mock", "run_forever", authenticated_user="u2", access_control_object="aco2")
        q_u1.get(authenticated_user="u2")

        # Delete ACO and verify that owner and superusers can still access it.
        remove("//sys/access_control_object_namespaces/queries/aco1")
        q_u1.get(authenticated_user="u1")
        q_u1.get(authenticated_user="superuser_u3")
        with raises_yt_error():
            q_u1.get(authenticated_user="u2")

    @authors("krock21")
    def test_aco_denies_author(self, query_tracker):
        create_user("u1")
        create_access_control_object(
            "aco_denies_author",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("deny", "u1", "use"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_denies_author")
        q_u1.get(authenticated_user="u1")

    @authors("krock21")
    def test_get(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_get",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "use"),
                    make_ace("allow", "u3", "read"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "administer"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_get")
        q_u1.get(authenticated_user="u2")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.get(authenticated_user="u3")

    @authors("krock21")
    def test_get_aco_in_response(self, query_tracker):
        create_user("u1")
        create_access_control_object(
            "aco_get_aco_in_response",
            "queries")
        q_active = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_get_aco_in_response")
        q_finished = start_query("mock", "complete_after", settings={"duration": 100}, authenticated_user="u1", access_control_object="aco_get_aco_in_response")
        q_without_aco_active = start_query("mock", "run_forever", authenticated_user="u1")
        q_without_aco_finished = start_query("mock", "complete_after", settings={"duration": 100}, authenticated_user="u1")

        wait(lambda: q_finished.get_state() == "completed")
        wait(lambda: q_without_aco_finished.get_state() == "completed")

        assert q_active.get()["access_control_object"] == "aco_get_aco_in_response"
        assert "access_control_object" not in q_active.get(attributes=["id"])

        assert q_finished.get()["access_control_object"] == "aco_get_aco_in_response"
        assert "access_control_object" not in q_finished.get(attributes=["id"])

        # We may want to change behaviour here to return "nobody" in this case
        assert "access_control_object" not in q_without_aco_active.get()
        assert "access_control_object" not in q_without_aco_active.get(attributes=["id"])

        assert "access_control_object" not in q_without_aco_finished.get()
        assert "access_control_object" not in q_without_aco_finished.get(attributes=["id"])

    @authors("krock21")
    def test_abort(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_abort",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "administer"),
                    make_ace("allow", "u3", "read"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_abort")
        wait(lambda: q_u1.get_state() == "running")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.abort(authenticated_user="u3")
        q_u1.abort(authenticated_user="u2")

    @authors("krock21")
    def test_get_query_result(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_get_query_result",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "read"),
                    make_ace("allow", "u3", "administer"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        q_u1 = start_query("mock", "complete_after", authenticated_user="u1", access_control_object="aco_get_query_result", settings={
            "duration": 3000,
            "results": [
                {"schema": [{"name": "foo", "type": "int64"}], "rows": [{"foo": 42}]},
            ]
        })
        q_u1.track()
        q_u1.get_result(0, authenticated_user="u2")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.get_result(0, authenticated_user="u3")

    @authors("krock21")
    def test_read_query_result(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_read_query_result",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "read"),
                    make_ace("allow", "u3", "administer"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        q_u1 = start_query("mock", "complete_after", authenticated_user="u1", access_control_object="aco_read_query_result", settings={
            "duration": 3000,
            "results": [
                {"schema": [{"name": "foo", "type": "int64"}], "rows": [{"foo": 42}]},
            ]
        })
        q_u1.track()
        q_u1.read_result(0, authenticated_user="u2")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.read_result(0, authenticated_user="u3")

    @authors("krock21")
    def test_alter(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_alter",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "administer"),
                    make_ace("allow", "u3", "read"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_alter")
        q_u1.alter(authenticated_user="u2", annotations={"qwe": "asd"})
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.alter(authenticated_user="u3", annotations={"qwe2": "asd3"})

    @authors("krock21", "mpereskokova")
    def test_alter_aco(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_alter_aco",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "administer"),
                    make_ace("allow", "u3", "read"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        create_access_control_object(
            "aco_alter_aco_u3",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u3", "administer"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_alter_aco")
        q_u2 = start_query("mock", "complete_after", settings={"duration": 0}, authenticated_user="u1", access_control_object="everyone")
        q_u2.track()

        q_u2.alter(authenticated_user="u1", access_control_object="nobody")
        q_u2.alter(authenticated_user="u1", annotations={"qwe": "asd"}, access_control_object="everyone")

        q_u1.alter(authenticated_user="u1", access_control_object="aco_alter_aco")

        q_u1.alter(authenticated_user="u2", annotations={"qwe": "asd"}, access_control_object="nobody")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.alter(authenticated_user="u2", annotations={"qwe": "asd"}, access_control_object="aco_alter_aco")

        q_u1.alter(authenticated_user="u1", access_control_object="aco_alter_aco")

        with raises_yt_error(AuthorizationErrorCode):
            q_u1.alter(authenticated_user="u3", access_control_object="nobody")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.alter(authenticated_user="u3", access_control_object="aco_alter_aco")

        q_u1.alter(authenticated_user="u1", access_control_object="aco_alter_aco_u3")

        q_u1.alter(authenticated_user="u3", access_control_object="aco_alter_aco")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.alter(authenticated_user="u3", access_control_object="aco_alter_aco_u3")

        with raises_yt_error(ResolveErrorCode):
            q_u1.alter(authenticated_user="u1", access_control_object="nonexistent_aco")

    @authors("mpereskokova")
    def test_alter_filter_factors(self, query_tracker):
        q1 = start_query("mock", "run_forever")
        q2 = start_query("mock", "complete_after", settings={"duration": 1000})
        q2.track()

        expect_queries([q1], list_queries(filter="run_forever"))
        expect_queries([q2], list_queries(filter="complete_after"))
        expect_queries([], list_queries(filter="asd"))

        q1.alter(annotations={"qwe": "asd"})
        q2.alter(annotations={"qwe": "asd"})

        expect_queries([q1], list_queries(filter="run_forever"))
        expect_queries([q2], list_queries(filter="complete_after"))
        expect_queries([q1, q2], list_queries(filter="asd"))


@pytest.mark.enabled_multidaemon
class TestGetQueryTrackerInfo(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }
    ENABLE_MULTIDAEMON = True
    SUPPORTED_FEATURES = {
        'access_control': True,
        'multiple_aco': True,
        'new_search_on_proxies': True,
        'new_search': True,
        'not_indexing': True,
        'not_indexing_on_proxies': True,
        'declare_params': True,
        'declare_params_on_proxies': True,
        'tutorials': True,
        'tutorials_on_proxies': True
    }

    @authors("aleksandr.gaev", "kirsiv40", "mpereskokova")
    def test_get_query_tracker_info(self, query_tracker):
        def check_qt_info(expected=None, **kwargs):
            info = get_query_tracker_info(**kwargs)
            assert isinstance(info.pop("expected_tables_version"), int)
            assert info == expected

        supported_features = self.SUPPORTED_FEATURES

        check_qt_info(
            expected={
                'query_tracker_stage': 'production',
                'cluster_name': 'primary',
                'supported_features': supported_features,
                'access_control_objects': ['everyone', 'everyone-share', 'nobody'],
                'clusters': ['primary'],
                'engines_info' : {},
            })
        check_qt_info(
            expected={
                'query_tracker_stage': 'production',
                'cluster_name': '',
                'supported_features': {},
                'access_control_objects': [],
                'clusters': [],
                'engines_info' : {},
            },
            attributes=[])
        check_qt_info(
            expected={
                'query_tracker_stage': 'production',
                'cluster_name': 'primary',
                'supported_features': {},
                'access_control_objects': [],
                'clusters': [],
                'engines_info' : {},
            },
            attributes=["cluster_name"])

        check_qt_info(
            expected={
                'query_tracker_stage': 'production',
                'cluster_name': '',
                'supported_features': supported_features,
                'access_control_objects': [],
                'clusters': [],
                'engines_info' : {},
            },
            attributes=["supported_features"])
        check_qt_info(
            expected={
                'query_tracker_stage': 'production',
                'cluster_name': '',
                'supported_features': {},
                'access_control_objects': ['everyone', 'everyone-share', 'nobody'],
                'clusters': [],
                'engines_info' : {},
            },
            attributes=["access_control_objects"])
        check_qt_info(
            expected={
                'query_tracker_stage': 'production',
                'cluster_name': '',
                'supported_features': {},
                'access_control_objects': [],
                'clusters': ['primary'],
                'engines_info' : {},
            },
            attributes=["clusters"])
        check_qt_info(
            expected={
                'query_tracker_stage': 'production',
                'cluster_name': '',
                'supported_features': {},
                'access_control_objects': [],
                'clusters': [],
                'engines_info' : {},
            },
            attributes=["engines_info"])
        check_qt_info(
            expected={
                'query_tracker_stage': 'production',
                'cluster_name': 'primary',
                'supported_features': supported_features,
                'access_control_objects': ['everyone', 'everyone-share', 'nobody'],
                'clusters': ['primary'],
                'engines_info' : {},
            },
            yql_agent_stage="some-invalid-stage")
        check_qt_info(
            expected={
                'query_tracker_stage': 'testing',
                'cluster_name': 'primary',
                'supported_features': supported_features,
                'access_control_objects': ['everyone', 'everyone-share', 'nobody'],
                'clusters': ['primary'],
                'engines_info' : {},
            },
            stage='testing')


@pytest.mark.enabled_multidaemon
class TestShare(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }
    ENABLE_MULTIDAEMON = True

    @authors("mpereskokova")
    def test_share(self, query_tracker):
        create_user("u1")
        create_user("u2")

        q = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="everyone-share")

        q.get(authenticated_user="u2")
        expect_queries([q], list_queries(authenticated_user="u1"))
        expect_queries([], list_queries(authenticated_user="u2"))


@pytest.mark.enabled_multidaemon
class TestSecrets(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }
    ENABLE_MULTIDAEMON = True

    @authors("mpereskokova")
    def test_secrets(self, query_tracker):
        secrets = [{'category': '', 'id': 'geheim', 'subcategory': '', 'ypath': '//tmp/secret_path_to_secret_value'}]

        q1 = start_query("mock", "complete_after", settings={"duration": 1000}, secrets=secrets)
        q1.track()
        q1_info = q1.get()
        assert q1_info["secrets"] == secrets

        q2 = start_query("mock", "fail", secrets=secrets)
        with raises_yt_error("failed"):
            q2.track()
        q2_info = q2.get()
        assert q2_info["secrets"] == secrets


@pytest.mark.enabled_multidaemon
class TestIndexTables(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }
    ENABLE_MULTIDAEMON = True

    @authors("mpereskokova")
    def test_start_query(self, query_tracker):
        create_user("user")

        q = start_query("mock", "complete_after", settings={"duration": 0}, access_control_objects=["everyone", "everyone-share"], authenticated_user="user")
        q.track()
        q_info = q.get()

        queries_by_aco = list(select_rows("* from [//sys/query_tracker/finished_queries_by_aco_and_start_time]"))
        assert len(queries_by_aco) == 2

        assert queries_by_aco[0]["query_id"] == q_info["id"]
        assert queries_by_aco[0]["access_control_object"] == "everyone"
        assert queries_by_aco[0]["minus_start_time"] == -date_string_to_timestamp_mcs(q_info["start_time"])

        assert queries_by_aco[1]["query_id"] == q_info["id"]
        assert queries_by_aco[1]["access_control_object"] == "everyone-share"
        assert queries_by_aco[1]["minus_start_time"] == -date_string_to_timestamp_mcs(q_info["start_time"])

        queries_by_user = list(select_rows("* from [//sys/query_tracker/finished_queries_by_user_and_start_time]"))
        assert len(queries_by_user) == 1

        assert queries_by_user[0]["user"] == "user"
        assert queries_by_user[0]["minus_start_time"] == -date_string_to_timestamp_mcs(q_info["start_time"])

    @authors("mpereskokova")
    def test_list_queries(self, query_tracker):
        create_user("u1")
        create_user("u2")

        q1 = start_query("mock", "complete_after", settings={"duration": 0}, access_control_objects=["everyone"], authenticated_user="u2")
        q2 = start_query("mock", "complete_after", settings={"duration": 0}, access_control_objects=["everyone", "everyone-share"], authenticated_user="u1")
        q1.track()
        q2.track()

        expect_queries([q2, q1], list_queries(authenticated_user="u1", limit=2))


@pytest.mark.enabled_multidaemon
class TestMultipleAccessControl(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }
    ENABLE_MULTIDAEMON = True

    @authors("mpereskokova")
    def test_start_query(self, query_tracker):
        create_user("u1")

        start_query("mock", "run_forever", authenticated_user="u1", access_control_object="nobody")
        start_query("mock", "run_forever", authenticated_user="u1", access_control_objects=["nobody"])
        with raises_yt_error():
            start_query("mock", "run_forever", authenticated_user="u1", access_control_objects=["nobody"], access_control_object="nobody")

    @authors("mpereskokova")
    def test_alter_aco(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_access_control_object(
            "aco",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u1", "read"),
                    make_ace("allow", "u1", "use"),
                ]
            })

        q = start_query("mock", "run_forever", authenticated_user="u2", access_control_object="nobody")
        with raises_yt_error(AuthorizationErrorCode):
            q.get(authenticated_user="u1")

        with raises_yt_error(AuthorizationErrorCode):
            q.alter(authenticated_user="u1", access_control_objects=["aco", "nobody"])
        q.alter(authenticated_user="u2", access_control_objects=["aco", "nobody"])

        q.get(authenticated_user="u1")

        q.alter(authenticated_user="u2", access_control_objects=[])
        with raises_yt_error(AuthorizationErrorCode):
            q.get(authenticated_user="u1")

    @authors("mpereskokova")
    def test_get_query(self, query_tracker):
        create_user("u1")
        create_access_control_object("yet_another_nobody", "queries")

        q1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="nobody")
        q2 = start_query("mock", "run_forever", authenticated_user="u1", access_control_objects=["nobody", "yet_another_nobody"])

        query = q1.get(authenticated_user="u1")
        assert query["access_control_object"] == "nobody"
        assert query["access_control_objects"] == ["nobody"]

        query = q2.get(authenticated_user="u1")
        assert "access_control_object" not in query
        assert query["access_control_objects"] == ["nobody", "yet_another_nobody"]

    @authors("mpereskokova")
    def test_list_queries(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_access_control_object("yet_another_nobody", "queries")
        create_access_control_object(
            "aco",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u1", "read"),
                    make_ace("allow", "u1", "use"),
                ]
            })

        q1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_objects=["nobody", "yet_another_nobody"])
        q2 = start_query("mock", "run_forever", authenticated_user="u2", access_control_objects=["aco"])

        expect_queries([q1, q2], list_queries(authenticated_user="u1"))
        expect_queries([q2], list_queries(authenticated_user="u2"))


@pytest.mark.enabled_multidaemon
class TestTutorials(YTEnvSetup):
    @authors("kirsiv40")
    def test_tutorials_are_not_listed_with_standart_queries(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3_superuser")
        add_member("u3_superuser", "superusers")
        create_access_control_object("yet_another_nobody", "queries")
        create_access_control_object(
            "aco",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u1", "read"),
                    make_ace("allow", "u1", "use"),
                ]
            })

        q1 = start_query(
            "mock",
            "some_query",
            draft=True,
            settings={"duration": 1000},
            access_control_objects=["nobody", "yet_another_nobody"],
        )
        q2 = start_query(
            "mock",
            "some_query",
            draft=True,
            settings={"duration": 1000},
            access_control_objects=["aco"],
        )
        q3 = start_query(
            "mock",
            "some_query",
            draft=True,
            settings={"duration": 1000},
            access_control_objects=["nobody", "yet_another_nobody"],
            annotations={"is_tutorial": False}
        )
        q4 = start_query(
            "mock",
            "some_query",
            draft=True,
            settings={"duration": 1000},
            access_control_objects=["aco"],
            annotations={"is_tutorial": "false"}
        )
        q5 = start_query(
            "mock",
            "some_query",
            draft=True,
            settings={"duration": 1000},
            access_control_objects=["nobody", "yet_another_nobody"],
            annotations={"is_tutorial": True}
        )
        q6 = start_query(
            "mock",
            "some_query",
            draft=True,
            settings={"duration": 1000},
            access_control_objects=["aco"],
            annotations={"is_tutorial": True}
        )

        def check_tutorial_queries():
            expect_queries([q2, q4], list_queries(authenticated_user="u1"))
            expect_queries([], list_queries(authenticated_user="u2"))
            expect_queries([q1, q2, q3, q4], list_queries(authenticated_user="u3_superuser"))

            expect_queries([q2, q4], list_queries(authenticated_user="u1", tutorial_filter=False))
            expect_queries([], list_queries(authenticated_user="u2", tutorial_filter=False))
            expect_queries([q1, q2, q3, q4], list_queries(authenticated_user="u3_superuser", tutorial_filter=False))

            expect_queries([q6], list_queries(authenticated_user="u1", tutorial_filter=True))
            expect_queries([], list_queries(authenticated_user="u2", tutorial_filter=True))
            expect_queries([q5, q6], list_queries(authenticated_user="u3_superuser", tutorial_filter=True))

        check_tutorial_queries()

    @authors("kirsiv40")
    def test_only_superusers_can_create_tutorials(self, query_tracker):
        create_user("u1_superuser")
        create_user("u2")
        add_member("u1_superuser", "superusers")

        u1q1 = start_query("mock", "complete_after", settings={"duration": 1000}, authenticated_user="u1_superuser", access_control_objects=["nobody"])
        u1q2 = start_query("mock", "complete_after", settings={"duration": 1000}, authenticated_user="u1_superuser", access_control_objects=["everyone"])
        u1q3 = start_query("mock", "some_query", draft=True, annotations={"is_tutorial": True}, authenticated_user="u1_superuser", access_control_objects=["nobody"])
        u1q4 = start_query("mock", "some_query", draft=True, annotations={"is_tutorial": True}, authenticated_user="u1_superuser", access_control_objects=["everyone"])

        u2q1 = start_query("mock", "complete_after", settings={"duration": 1000}, authenticated_user="u2", access_control_objects=["nobody"])
        u2q2 = start_query("mock", "complete_after", settings={"duration": 1000}, authenticated_user="u2", access_control_objects=["everyone"])
        u2q3 = start_query("mock", "complete_after", settings={"duration": 1000}, annotations={"is_tutorial": False}, authenticated_user="u2", access_control_objects=["nobody"])
        u2q4 = start_query("mock", "complete_after", settings={"duration": 1000}, annotations={"is_tutorial": False}, authenticated_user="u2", access_control_objects=["everyone"])

        with raises_yt_error("superuser"):
            start_query("mock", "some_query", draft=True, annotations={"is_tutorial": True}, authenticated_user="u2", access_control_objects=["nobody"])
        with raises_yt_error("superuser"):
            start_query("mock", "some_query", draft=True, annotations={"is_tutorial": True}, authenticated_user="u2", access_control_objects=["everyone"])

        def check_tutorial_queries():
            expect_queries([u1q1, u1q2, u2q1, u2q2, u2q3, u2q4], list_queries(authenticated_user="u1_superuser"))
            expect_queries([u1q2, u2q1, u2q2, u2q3, u2q4], list_queries(authenticated_user="u2"))

            expect_queries([u1q1, u1q2, u2q1, u2q2, u2q3, u2q4], list_queries(authenticated_user="u1_superuser", tutorial_filter=False))
            expect_queries([u1q2, u2q1, u2q2, u2q3, u2q4], list_queries(authenticated_user="u2", tutorial_filter=False))

            expect_queries([u1q3, u1q4], list_queries(authenticated_user="u1_superuser", tutorial_filter=True))
            expect_queries([u1q4], list_queries(authenticated_user="u2", tutorial_filter=True))

        check_tutorial_queries()

        u1q1.track()
        u1q2.track()
        u2q1.track()
        u2q2.track()
        u2q3.track()
        u2q4.track()

        check_tutorial_queries()

    @authors("mpereskokova")
    def test_alter_tutorials(self, query_tracker):
        create_user("u1_superuser")
        create_user("u2")
        add_member("u1_superuser", "superusers")

        with raises_yt_error(""):
            start_query("mock", "complete_after", annotations={"is_tutorial": True}, settings={"duration": 1000}, authenticated_user="u1_superuser", access_control_objects=["everyone"])

        from_tutorial_to_ordinary = start_query("mock", "some_query", draft=True, annotations={"is_tutorial": True}, authenticated_user="u1_superuser", access_control_objects=["everyone"])
        from_tutorial_to_tutorial = start_query("mock", "some_query", draft=True, annotations={"is_tutorial": True}, authenticated_user="u1_superuser", access_control_objects=["nobody"])
        from_ordinary_to_tutorial = start_query("mock", "some_query", draft=True, authenticated_user="u1_superuser", access_control_objects=["everyone"])
        ordinary_not_tutorial = start_query("mock", "complete_after", settings={"duration": 1000}, authenticated_user="u1_superuser", access_control_objects=["nobody"])
        ordinary_not_superuser = start_query("mock", "some_query", draft=True, authenticated_user="u2", access_control_objects=["everyone"])

        # check that there are no tutorials in common search
        expect_queries([from_ordinary_to_tutorial, ordinary_not_tutorial, ordinary_not_superuser], list_queries(authenticated_user="u1_superuser"))
        expect_queries([from_ordinary_to_tutorial, ordinary_not_superuser], list_queries(authenticated_user="u2"))
        expect_queries([from_ordinary_to_tutorial, ordinary_not_tutorial, ordinary_not_superuser], list_queries(authenticated_user="u1_superuser", tutorial_filter=False))
        expect_queries([from_ordinary_to_tutorial, ordinary_not_superuser], list_queries(authenticated_user="u2", tutorial_filter=False))
        expect_queries([from_tutorial_to_ordinary, from_tutorial_to_tutorial], list_queries(authenticated_user="u1_superuser", tutorial_filter=True))
        expect_queries([from_tutorial_to_ordinary], list_queries(authenticated_user="u2", tutorial_filter=True))

        from_tutorial_to_ordinary.alter(annotations={"is_tutorial": False}, authenticated_user="u1_superuser")
        from_tutorial_to_tutorial.alter(access_control_objects=["everyone"], authenticated_user="u1_superuser")
        from_ordinary_to_tutorial.alter(annotations={"is_tutorial": True}, authenticated_user="u1_superuser")
        from_ordinary_to_tutorial.get()
        with raises_yt_error("Tutorials should be in draft state"):
            ordinary_not_tutorial.alter(annotations={"is_tutorial": True}, authenticated_user="u1_superuser")
        with raises_yt_error("superuser"):
            ordinary_not_superuser.alter(annotations={"is_tutorial": True}, authenticated_user="u2")

        # check that there are no tutorials in common search
        expect_queries([from_tutorial_to_ordinary, ordinary_not_tutorial, ordinary_not_superuser], list_queries(authenticated_user="u1_superuser"))
        expect_queries([from_tutorial_to_ordinary, ordinary_not_superuser], list_queries(authenticated_user="u2"))
        expect_queries([from_tutorial_to_ordinary, ordinary_not_tutorial, ordinary_not_superuser], list_queries(authenticated_user="u1_superuser", tutorial_filter=False))
        expect_queries([from_tutorial_to_ordinary, ordinary_not_superuser], list_queries(authenticated_user="u2", tutorial_filter=False))
        expect_queries([from_tutorial_to_tutorial, from_ordinary_to_tutorial], list_queries(authenticated_user="u1_superuser", tutorial_filter=True))
        expect_queries([from_tutorial_to_tutorial, from_ordinary_to_tutorial], list_queries(authenticated_user="u2", tutorial_filter=True))


# Separate list to fit 480 seconds limit for a test class.
@pytest.mark.enabled_multidaemon
class TestAccessControlList(YTEnvSetup):
    NUM_TEST_PARTITIONS = 16

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    ENABLE_MULTIDAEMON = True

    @pytest.mark.parametrize(
        "query_type,filter",
        [
            (["mock", "fail", {}], None),
            (["mock", "complete_after", {"settings": {"duration": 1000}}], None),
            (["mock", "fail_by_exception", {}], None),
            (["mock", "blahblah", {}], None),
            (["mock", "fail_after", {"settings": {"duration": 1000}}], None),
            (["mock", "run_forever", {}], None),
            (["mock", "fail", {}], "fail"),
            (["mock", "run_forever", {}], "run_forever"),
        ])
    @authors("krock21")
    def test_list(self, query_tracker, query_type, filter):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_user("u4")
        create_user("u5")
        create_user("u6")
        create_user("superuser_u7")
        add_member("superuser_u7", "superusers")
        create_access_control_object(
            "aco_list_u1",
            "queries",
            attributes={
                "principal_acl": [
                    # Allow u2, deny everyone else.
                    make_ace("allow", "u2", "use"),
                    make_ace("allow", "u3", "read"),
                    make_ace("allow", "u4", "write"),
                    make_ace("allow", "u5", "administer"),
                ]
            })
        create_access_control_object(
            "aco_list_u2",
            "queries",
            attributes={
                "principal_acl": [
                    # Allow u3, deny everyone else.
                    make_ace("allow", "u3", "use"),
                    make_ace("allow", "u4", "use"),
                    make_ace("deny", "u4", "use"),
                    make_ace("deny", "u5", "use"),
                ]
            })
        create_access_control_object(
            "aco_list_u3",
            "queries",
            attributes={
                "principal_acl": [
                    # Allow everyone except u6.
                    make_ace("allow", "u1", "use"),
                    make_ace("allow", "u2", "use"),
                    make_ace("allow", "u4", "use"),
                    make_ace("allow", "u5", "use"),
                ]
            })
        create_access_control_object(
            "aco_list_u4",
            "queries",
            attributes={
                "principal_acl": [
                    # Allow everyone except u5.
                    make_ace("allow", "u1", "use"),
                    make_ace("allow", "u2", "use"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        create_access_control_object(
            "aco_list_u5",
            "queries",
            attributes={
                "principal_acl": [
                    # Deny everyone implicitly.
                ]
            })

        create_access_control_object(
            "aco_list_u6",
            "queries",
            attributes={
                "principal_acl": [
                    # Deny everyone explicitly, even u6 themselves.
                    make_ace("deny", "u1", "use"),
                    make_ace("deny", "u2", "use"),
                    make_ace("deny", "u3", "use"),
                    make_ace("deny", "u4", "use"),
                    make_ace("deny", "u5", "use"),
                    make_ace("deny", "u6", "use"),
                ]
            })

        q_u1 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u1", access_control_object="aco_list_u1")
        q_u2 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u2", access_control_object="aco_list_u2")
        q_u3 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u3", access_control_object="aco_list_u3")
        q_u4 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u4", access_control_object="aco_list_u4")
        q_u5 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u5", access_control_object="aco_list_u5")
        q_u6 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u6", access_control_object="aco_list_u6")

        if "settings" in query_type[2] and "duration" in query_type[2]["settings"]:
            q_u1.track(raise_on_unsuccess=False)
            q_u2.track(raise_on_unsuccess=False)
            q_u3.track(raise_on_unsuccess=False)
            q_u4.track(raise_on_unsuccess=False)
            q_u5.track(raise_on_unsuccess=False)
            q_u6.track(raise_on_unsuccess=False)

        # Root user sees everything.
        expect_queries([q_u1, q_u2, q_u3, q_u4, q_u5, q_u6], list_queries(cursor_direction="future"))
        # Superuser sees everything.
        expect_queries([q_u1, q_u2, q_u3, q_u4, q_u5, q_u6], list_queries(cursor_direction="future", authenticated_user="superuser_u7"))
        expect_queries([q_u1, q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u1"))
        expect_queries([q_u1, q_u2, q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u2"))
        expect_queries([q_u2, q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u3"))
        expect_queries([q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u4"))
        expect_queries([q_u3, q_u5], list_queries(cursor_direction="future", authenticated_user="u5"))
        expect_queries([q_u6], list_queries(cursor_direction="future", authenticated_user="u6"))

        # Root user sees everything.
        expect_queries([q_u1, q_u2, q_u3, q_u4, q_u5, q_u6], list_queries(filter=filter, cursor_direction="future", use_full_text_search=True))
        # Superuser sees everything.
        expect_queries([q_u1, q_u2, q_u3, q_u4, q_u5, q_u6], list_queries(filter=filter, cursor_direction="future", authenticated_user="superuser_u7", use_full_text_search=True))
        expect_queries([q_u1, q_u3, q_u4], list_queries(filter=filter, cursor_direction="future", authenticated_user="u1", use_full_text_search=True))
        expect_queries([q_u1, q_u2, q_u3, q_u4], list_queries(filter=filter, cursor_direction="future", authenticated_user="u2", use_full_text_search=True))
        expect_queries([q_u2, q_u3, q_u4], list_queries(filter=filter, cursor_direction="future", authenticated_user="u3", use_full_text_search=True))
        expect_queries([q_u3, q_u4], list_queries(filter=filter, cursor_direction="future", authenticated_user="u4", use_full_text_search=True))
        expect_queries([q_u3, q_u5], list_queries(filter=filter, cursor_direction="future", authenticated_user="u5", use_full_text_search=True))
        expect_queries([q_u6], list_queries(filter=filter, cursor_direction="future", authenticated_user="u6", use_full_text_search=True))

    @authors("krock21")
    def test_list_by_aco(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_list_by_aco1",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "users", "use"),
                ]
            })
        create_access_control_object(
            "aco_list_by_aco2",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "users", "use"),
                ]
            })
        create_access_control_object(
            "aco_list_by_aco3",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "users", "use"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_list_by_aco1")
        q_u2 = start_query("mock", "run_forever", authenticated_user="u2", access_control_object="aco_list_by_aco2")
        q_u3 = start_query("mock", "run_forever", authenticated_user="u3", access_control_object="aco_list_by_aco3")

        def check_filter_by_aco(query, num):
            expect_queries([], list_queries(filter="aco:", use_full_text_search=True))
            expect_queries([], list_queries(filter="\"aco:\"", use_full_text_search=True))
            expect_queries([], list_queries(filter=f"aco_list_by_aco{num}", use_full_text_search=True))
            expect_queries([query], list_queries(filter=f"\"aco:aco_list_by_aco{num}\"", use_full_text_search=True))
            expect_queries([query], list_queries(filter=f"'aco:aco_list_by_aco{num}'", use_full_text_search=True))

        check_filter_by_aco(q_u1, 1)
        check_filter_by_aco(q_u2, 2)
        check_filter_by_aco(q_u3, 3)

    @authors("mpereskokova")
    def test_list_sql_injection(self, query_tracker):
        create_user("u1")
        create_user("u2")
        q0 = start_query("mock", "fail", authenticated_user="u1")
        q1 = start_query("mock", "fail", authenticated_user="u2")

        expect_queries([q0], list_queries(cursor_direction="future", attributes=["id"], user="u1"))
        expect_queries([q1], list_queries(cursor_direction="future", attributes=["id"], user="u2"))
        expect_queries([], list_queries(cursor_direction="future", attributes=["id"], user="u1\") OR ([user]=\"u2"))


@pytest.mark.enabled_multidaemon
class TestSearch(YTEnvSetup):
    @authors("kirsiv40")
    @pytest.mark.timeout(900)
    def test_list_search_filters(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("superuser_u3")
        add_member("superuser_u3", "superusers")

        create_access_control_object(
            "some-aco'$%^'",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "use"),
                ]
            })
        q0 = start_query("mock", "select * from `//some/query/with/tables/first`", authenticated_user="u1")
        q1 = start_query("mock", "select * from [//some/other/query/with/tables]", authenticated_user="u2", access_control_object="everyone")
        q2 = start_query("mock", "select * from [//some/OTHER/query/with/tables]", authenticated_user="u2", access_control_objects=["nobody", "some-aco'$%^'"])
        q3 = start_query("mock", "select * from //some--broken--query/with/comments and select prefix 'sel'", authenticated_user="superuser_u3")

        def collect_batch(attribute):
            queries = list_queries(cursor_direction="future", attributes=["id", attribute])
            result = []
            for q, expected_q in zip(queries["queries"], (q0, q1, q2, q3)):
                assert q["id"] == expected_q.id
                result.append(q[attribute])
            return result

        q_times = collect_batch("start_time")

        exclude_by_visibility_map = {
            None: {},
            "u1": {q2, q3},
            "u2": {q0, q3},
            "superuser_u3": {},
        }

        exclude_by_user_filter_map = {
            None: {},
            "u1": {q1, q2, q3},
            "u2": {q0, q3},
            "superuser_u3": {q0, q1, q2},
        }

        exclude_by_engine_filter_map = {
            None: {},
            "mock": {},
            "yql": {q0, q1, q2, q3},
        }

        exclude_by_cursor_time_map = {
            None: {},
            q_times[1]: {q0, q1},
        }

        def expect_with_filters(queries_list, authenticated_user=None, user=None, engine=None, cursor_time=None, **kwargs):
            kwargs["authenticated_user"] = authenticated_user
            kwargs["user"] = user
            kwargs["engine"] = engine
            kwargs["cursor_time"] = cursor_time
            expect_queries([query for query in queries_list if
                            query not in exclude_by_user_filter_map[user] and
                            query not in exclude_by_visibility_map[authenticated_user] and
                            query not in exclude_by_engine_filter_map[engine] and
                            query not in exclude_by_cursor_time_map[cursor_time]], list_queries(**kwargs))

        params_map = {"use_full_text_search": True}
        for authenticated_user in exclude_by_visibility_map:
            params_map["authenticated_user"] = authenticated_user
            for user in exclude_by_user_filter_map:
                params_map["user"] = user
                for engine in exclude_by_engine_filter_map:
                    params_map["engine"] = engine
                    for cursor_time in exclude_by_cursor_time_map:
                        params_map["cursor_time"] = cursor_time
                        print_debug(f"AuthenticatedUser: {authenticated_user}, UserFilter: {user}, Engine: {engine}")
                        expect_with_filters([q0, q1, q2, q3], cursor_direction="future", attributes=["id"], filter="some some some", **params_map)
                        expect_with_filters([q1, q2], cursor_direction="future", attributes=["id"], filter="other", **params_map)
                        expect_with_filters([], cursor_direction="future", attributes=["id"], filter="select", **params_map)

                        expect_with_filters([q0, q1, q2, q3], cursor_direction="future", attributes=["id"], filter="so", search_by_token_prefix=True, **params_map)
                        expect_with_filters([q1, q2], cursor_direction="future", attributes=["id"], filter="oth", search_by_token_prefix=True, **params_map)
                        expect_with_filters([q3], cursor_direction="future", attributes=["id"], filter="se", search_by_token_prefix=True, **params_map)

                        expect_with_filters([q0, q1, q2, q3], cursor_direction="future", attributes=["id"], filter="other first broken", **params_map)
                        expect_with_filters([q0, q1, q2, q3], cursor_direction="future", attributes=["id"], filter="ot fir broke", search_by_token_prefix=True, **params_map)

                        expect_with_filters([q1, q2], cursor_direction="future", attributes=["id"], filter="'aco:'", search_by_token_prefix=True, **params_map)
                        expect_with_filters([q2], cursor_direction="future", attributes=["id"], filter="`aco:some-aco'$%^'`", search_by_token_prefix=True, **params_map)
                        expect_with_filters([], cursor_direction="future", attributes=["id"], filter="'aco:random-aco'", search_by_token_prefix=True, **params_map)
                        expect_with_filters([q1, q2], cursor_direction="future", attributes=["id"], filter="\"aco:some-aco'$%^'\" 'aco:everyone'", search_by_token_prefix=True, **params_map)


@pytest.mark.enabled_multidaemon
class TestTTL(YTEnvSetup):
    QUERY_TRACKER_DYNAMIC_CONFIG = {"not_indexed_queries_ttl": 1000}

    @authors("mpereskokova")
    def test_ttl(self, query_tracker):
        set("//sys/query_tracker/finished_queries/@min_data_ttl", 1000)
        set("//sys/query_tracker/finished_query_results/@min_data_ttl", 1000)

        q0 = start_query("mock", "complete_after", settings={"duration": 1000, 'is_indexed': False})
        q1 = start_query("mock", "complete_after", settings={"duration": 1000, 'is_indexed': True})

        q0.track()
        q1.track()

        expect_queries([q1], list_queries())
        q0.get()
        q1.get()

        time.sleep(3)  # > row TTL (1 second)
        sync_compact_table("//sys/query_tracker/finished_queries")
        sync_compact_table("//sys/query_tracker/finished_query_results")

        expect_queries([q1], list_queries())
        q1.get()
        with raises_yt_error(""):
            q0.get()


##################################################################


@authors("apollo1321")
@pytest.mark.enabled_multidaemon
class TestQueriesMockRpcProxy(TestQueriesMock):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_MULTIDAEMON = True


@authors("mpereskokova")
@pytest.mark.enabled_multidaemon
class TestQueryTrackerBanRpcProxy(TestQueryTrackerBan):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_MULTIDAEMON = True


@authors("lucius")
@pytest.mark.enabled_multidaemon
class TestQueryTrackerResultsRpcProxy(TestQueryTrackerResults):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_MULTIDAEMON = True


@authors("apollo1321")
@pytest.mark.enabled_multidaemon
class TestAccessControlRpcProxy(TestAccessControl):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_MULTIDAEMON = True


@authors("mpereskokova")
@pytest.mark.enabled_multidaemon
class TestShareRpcProxy(TestShare):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_MULTIDAEMON = True


@authors("mpereskokova")
@pytest.mark.enabled_multidaemon
class TestSecretsRpcProxy(TestSecrets):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_MULTIDAEMON = True


@authors("apollo1321")
@pytest.mark.enabled_multidaemon
class TestAccessControlListRpcProxy(TestAccessControlList):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_MULTIDAEMON = True


@authors("mpereskokova")
@pytest.mark.enabled_multidaemon
class TestMultipleAccessControlRpcProxy(TestMultipleAccessControl):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_MULTIDAEMON = True


@authors("mpereskokova")
@pytest.mark.enabled_multidaemon
class TestTutorialsRpcProxy(TestTutorials):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_MULTIDAEMON = True


@authors("mpereskokova")
@pytest.mark.enabled_multidaemon
class TestGetQueryTrackerInfoRpcProxy(TestGetQueryTrackerInfo):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_MULTIDAEMON = True
    SUPPORTED_FEATURES = {
        'access_control': True,
        'multiple_aco': True,
        'new_search': True,
        'not_indexing': True,
        'declare_params': True,
        'tutorials': True
    }


@authors("kirsiv40")
@pytest.mark.enabled_multidaemon
class TestSearchRpcProxy(TestSearch):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_MULTIDAEMON = True
