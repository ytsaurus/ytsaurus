from yt_commands import (authors, raises_yt_error, create_access_control_object_namespace,
                         create_access_control_object, create_user, make_ace, set as yt_set,
                         remove)

from base import ClickHouseTestBase, Clique, QueryFailedError

import time


class TestSqlUdf(ClickHouseTestBase):
    def setup_method(self, method):
        super().setup_method(method)
        create_access_control_object_namespace(name="chyt")
        create_access_control_object(name="clique", namespace="chyt")

    @authors("gudqeit")
    def test_permissions_to_create_function(self):
        create_user("u1")
        create_user("u2")
        acl = [make_ace("allow", "u1", "manage")]
        yt_set("//sys/access_control_object_namespaces/chyt/clique/principal/@acl", acl)

        with Clique(1, alias="*clique") as clique:
            query = "create function linear_equation as (x, k, b) -> k*x + b"

            with raises_yt_error("Access denied"):
                clique.make_query(query, user="u2")

            clique.make_query(query, user="u1")

            time.sleep(0.4)

            with raises_yt_error("Access denied"):
                clique.make_query("drop function linear_equation", user="u2")

            clique.make_query("drop function linear_equation", user="u1")

    @authors("gudqeit")
    def test_simple_udf(self):
        with Clique(1, alias="*clique") as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query("select number, linear_equation(number, 2, 1) from numbers(3)")

            clique.make_query("create function linear_equation as (x, k, b) -> k*x + b")

            time.sleep(0.4)

            query = "select number, linear_equation(number, 2, 1) as result from numbers(2)"
            assert clique.make_query(query) == [
                {"number": 0, "result": 1},
                {"number": 1, "result": 3},
            ]

    @authors("gudqeit")
    def test_udf_is_registered_on_each_instance(self):
        with Clique(2, alias="*clique") as clique:
            clique.make_query("create function linear_equation as (x, k, b) -> k*x + b")

            time.sleep(0.4)

            query = "select number, linear_equation(number, 2, 1) as result from numbers(2)"

            instances = clique.get_active_instances()
            for instance in instances:
                assert clique.make_direct_query(instance, query) == [
                    {"number": 0, "result": 1},
                    {"number": 1, "result": 3},
                ]

    @authors("gudqeit")
    def test_drop_udf(self):
        with Clique(1, alias="*clique") as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query("drop function linear_equation")

            clique.make_query("drop function if exists linear_equation")

            clique.make_query("create function linear_equation as (x, k, b) -> k*x + b")

            time.sleep(0.4)

            query = "select number, linear_equation(number, 2, 1) as result from numbers(1)"
            assert clique.make_query(query) == [{"number": 0, "result": 1}]

            clique.make_query("drop function linear_equation")

            time.sleep(0.4)

            with raises_yt_error(QueryFailedError):
                clique.make_query("select number, linear_equation(number, 2, 1) from numbers(3)")

    @authors("gudqeit")
    def test_replace_udf(self):
        with Clique(2, alias="*clique") as clique:
            clique.make_query("create function linear_equation as (x, k, b) -> k*x + b")

            time.sleep(0.4)

            query = "select number, linear_equation(number, 2, 1) as result from numbers(1)"
            assert clique.make_query(query) == [{"number": 0, "result": 1}]

            clique.make_query("create or replace function linear_equation as (x, k) -> k*x")

            time.sleep(0.4)

            query = "select number, linear_equation(number, 2) as result from numbers(1)"

            instances = clique.get_active_instances()
            for instance in instances:
                assert clique.make_direct_query(instance, query) == [{"number": 0, "result": 0}]

    @authors("gudqeit")
    def test_bad_udf_name(self):
        with Clique(1, alias="*clique") as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query('create function "@acl" as (x, k) -> k*x')

            with raises_yt_error(QueryFailedError):
                clique.make_query('create function "some/path" as (x, k) -> k*x')

    @authors("gudqeit")
    def test_bad_query_in_cypress(self):
        with Clique(1, alias="*clique") as clique:
            clique.make_query("create function linear_equation as (x, k, b) -> k*x + b")

            bad_query = "create function linear_equation as (x, k, b) ->"
            udf_path = "//sys/strawberry/chyt/clique/user_defined_sql_functions/linear_equation"
            yt_set(udf_path, bad_query)

            time.sleep(0.4)

            with raises_yt_error("Failed to parse user defined function linear_equation"):
                clique.make_query("select linear_equation() as result from numbers(1)")

            clique.make_query("drop function linear_equation")

    @authors("gudqeit")
    def test_failed_object_sync(self):
        with Clique(1, alias="*clique") as clique:
            clique.make_query("create function linear_equation as (x, k) -> k*x")

            time.sleep(0.4)

            clique.make_query("select linear_equation(number, 5) as result from numbers(1)")

            remove(clique.sql_udf_path)

            time.sleep(1.5)

            with raises_yt_error("Unknown function linear_equation"):
                clique.make_query("select linear_equation(number, 5) as result from numbers(1)")
