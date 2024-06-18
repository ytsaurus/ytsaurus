from yt_env_setup import YTEnvSetup

from yt_commands import (
    add_member, authors, create_access_control_object, create_access_control_object_namespace, remove,
    make_ace, raises_yt_error, wait, create_user, print_debug, select_rows)

from yt_error_codes import AuthorizationErrorCode, ResolveErrorCode

from yt_queries import start_query, list_queries, get_query_tracker_info

from yt.test_helpers import assert_items_equal

from collections import Counter

import pytest


def expect_queries(queries, list_result, incomplete=False):
    try:
        assert set(q.id for q in queries) == set(q["id"] for q in list_result["queries"])
        assert list_result["incomplete"] == incomplete
    except AssertionError:
        timestamp = list_result["timestamp"]
        print_debug(f"Assertion failed, dumping content of dynamic tables by timestamp {timestamp}")
        for table in ("active_queries", "finished_queries", "finished_queries_by_start_time"):
            print_debug(f"{table}:")
            select_rows(f"* from [//sys/query_tracker/{table}]", timestamp=timestamp)
        raise


class TestQueriesMock(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

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

    @authors("max42")
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

            expect_queries([q4, q2, q0], list_queries(filter="fail"))

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
        assert len(list_queries(filter="bar")["queries"]) == 1

        q.alter(annotations={"qux": "quux"})
        q_info = list_queries(filter="qux", attributes=["annotations", "state"])["queries"][0]
        assert q_info["annotations"] == {"qux": "quux"}
        assert q_info["state"] == "running"
        wait(lambda: q.get_state() == "completed")

        assert len(list_queries(filter="qux")["queries"]) > 0

        q.alter(annotations={"qwe": "asd"})
        assert len(list_queries(filter="asd")["queries"]) > 0

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


class TestAccessControl(YTEnvSetup):
    NUM_TEST_PARTITIONS = 16

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

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
        with raises_yt_error("Error while fetching access control object queries/aco1"):
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

    @authors("krock21")
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

    @authors("aleksandr.gaev")
    def test_get_query_tracker_info(self, query_tracker):
        assert get_query_tracker_info() == {'cluster_name': 'primary', 'supported_features': {'access_control': True}, 'access_control_objects': ['nobody']}

        remove("//sys/access_control_object_namespaces/queries/nobody")
        remove("//sys/access_control_object_namespaces/queries")

        assert get_query_tracker_info() == {'cluster_name': 'primary', 'supported_features': {'access_control': False}, 'access_control_objects': []}

        create_access_control_object_namespace("queries")
        create_access_control_object("nobody", "queries")

        assert get_query_tracker_info(attributes=[]) == {'cluster_name': '', 'supported_features': {}, 'access_control_objects': []}
        assert get_query_tracker_info(attributes=["cluster_name"]) == {'cluster_name': 'primary', 'supported_features': {}, 'access_control_objects': []}
        assert get_query_tracker_info(attributes=["supported_features"]) == {'cluster_name': '', 'supported_features': {'access_control': True}, 'access_control_objects': []}
        assert get_query_tracker_info(attributes=["access_control_objects"]) == {'cluster_name': '', 'supported_features': {}, 'access_control_objects': ['nobody']}


# Separate list to fit 480 seconds limit for a test class.
class TestAccessControlList(YTEnvSetup):
    NUM_TEST_PARTITIONS = 16

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @pytest.mark.parametrize(
        "query_type",
        [
            ["mock", "fail", {}],
            ["mock", "complete_after", {"settings": {"duration": 1000}}],
            ["mock", "fail_by_exception", {}],
            ["mock", "blahblah", {}],
            ["mock", "fail_after", {"settings": {"duration": 1000}}],
            ["mock", "run_forever", {}]
        ])
    @authors("krock21")
    def test_list(self, query_tracker, query_type):
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

        expect_queries([q_u1, q_u2, q_u3, q_u4, q_u5, q_u6], list_queries(cursor_direction="future"))  # Root user sees everything.
        expect_queries([q_u1, q_u2, q_u3, q_u4, q_u5, q_u6], list_queries(cursor_direction="future", authenticated_user="superuser_u7"))  # Superuser sees everything.
        expect_queries([q_u1, q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u1"))
        expect_queries([q_u1, q_u2, q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u2"))
        expect_queries([q_u2, q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u3"))
        expect_queries([q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u4"))
        expect_queries([q_u3, q_u5], list_queries(cursor_direction="future", authenticated_user="u5"))
        expect_queries([q_u6], list_queries(cursor_direction="future", authenticated_user="u6"))

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

        expect_queries([q_u1, q_u2, q_u3], list_queries(filter="aco:"))
        expect_queries([q_u1], list_queries(filter="aco_list_by_aco1"))
        expect_queries([q_u1], list_queries(filter="aco:aco_list_by_aco1"))
        expect_queries([q_u2], list_queries(filter="aco_list_by_aco2"))
        expect_queries([q_u2], list_queries(filter="aco:aco_list_by_aco2"))
        expect_queries([q_u3], list_queries(filter="aco_list_by_aco3"))
        expect_queries([q_u3], list_queries(filter="aco:aco_list_by_aco3"))


##################################################################


@authors("apollo1321")
class TestQueriesMockRpcProxy(TestQueriesMock):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


@authors("apollo1321")
class TestAccessControlRpcProxy(TestAccessControl):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


@authors("apollo1321")
class TestAccessControlListRpcProxy(TestAccessControlList):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
