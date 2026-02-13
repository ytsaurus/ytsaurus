from functools import partial
from yt_env_setup import YTEnvSetup
from yt_commands import (
    alter_table,
    authors,
    create_user,
    create,
    join_reduce,
    ls,
    make_ace,
    map_reduce,
    map,
    merge,
    raises_yt_error,
    read_table,
    reduce,
    remove,
    set as yt_set,
    sort,
    sorted_dicts,
    wait,
    write_file,
    write_table,
)
import pytest
from random import Random
from yt_type_helpers import make_column, make_sorted_column, optional_type

from textwrap import dedent


@authors("coteeq")
@pytest.mark.enabled_multidaemon
@pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
class TestSchedulerRowLevelSecurityCommands(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @staticmethod
    def _make_rl_ace(subjects, row_access_predicate, incompatible_row_access_predicate_mode=None):
        ace = make_ace("allow", subjects, "read")
        ace["row_access_predicate"] = row_access_predicate
        if incompatible_row_access_predicate_mode:
            ace["incompatible_row_access_predicate_mode"] = incompatible_row_access_predicate_mode
        return ace

    @staticmethod
    def _create_users():
        create_user("prime_user")
        create_user("full_read_user")
        create_user("basic_read_user")

    @classmethod
    def _get_acl(cls):
        return [
            cls._make_rl_ace("prime_user", "int in (2, 3)"),
            cls._make_rl_ace("prime_user", "int in (5, 7)"),
            make_ace("allow", "full_read_user", "full_read"),
            make_ace("allow", ["prime_user", "full_read_user", "basic_read_user"], "read"),
        ]

    @staticmethod
    def _rows(*int_seq):
        return [{"int": i, "str": f"val_{i}"} for i in int_seq]

    @classmethod
    def _create_table(cls, optimize_for, sorted: bool = False, path="//tmp/t"):
        create(
            "table",
            path,
            attributes={
                "inherit_acl": False,
                "acl": cls._get_acl(),
                "schema": [
                    make_sorted_column("int", "int64") if sorted else make_column("int", "int64"),
                    make_column("str", "string"),
                ],
                "optimize_for": optimize_for,
            },
        )

    @classmethod
    def _prepare_simple_test(cls, optimize_for, sorted=False):
        cls._create_users()
        cls._create_table(optimize_for, sorted)
        write_table("//tmp/t", cls._rows(*range(5)))
        write_table("<append=%true>//tmp/t", cls._rows(*range(5, 10)))

    def test_no_omit_inaccessible_rows(self, optimize_for):
        self._prepare_simple_test(optimize_for)
        with raises_yt_error("Access denied for user \"prime_user\""):
            merge(
                in_="//tmp/t",
                out="<create=%true>//tmp/t_out",
                mode="unordered",
                authenticated_user="prime_user",
            )

    @pytest.mark.parametrize("order", ["ordered", "unordered", "sorted"])
    def test_merge_simple(self, optimize_for, order):
        self._prepare_simple_test(optimize_for, sorted=order == "sorted")
        merge_by = {"merge_by": ["int"]} if order == "sorted" else {}
        merge(
            in_="//tmp/t",
            out="<create=%true>//tmp/t_out",
            spec={
                "omit_inaccessible_rows": True,
            },
            mode=order,
            authenticated_user="prime_user",
            **merge_by,
        )
        if order in ("sorted", "ordered"):
            assert read_table("//tmp/t_out") == self._rows(2, 3, 5, 7)
        else:
            assert sorted_dicts(read_table("//tmp/t_out")) == self._rows(2, 3, 5, 7)

    @pytest.mark.parametrize("order", ["ordered", "unordered"])
    def test_map_simple(self, optimize_for, order):
        self._prepare_simple_test(optimize_for)
        map(
            in_="//tmp/t",
            out="<create=%true>//tmp/t_out",
            spec={
                "omit_inaccessible_rows": True,
                "ordered": order == "ordered",
                "format": "yson",
            },
            authenticated_user="prime_user",
            mapper_command="cat",
        )
        if order == "ordered":
            assert read_table("//tmp/t_out") == self._rows(2, 3, 5, 7)
        else:
            assert sorted_dicts(read_table("//tmp/t_out")) == self._rows(2, 3, 5, 7)

    def test_sorted_merge_multiple_inputs(self, optimize_for):
        self._create_users()
        self._create_table(optimize_for, sorted=True, path="//tmp/t1")
        self._create_table(optimize_for, sorted=True, path="//tmp/t2")

        write_table("<append=%true>//tmp/t1", self._rows(*range(5)))
        write_table("<append=%true>//tmp/t1", self._rows(*range(5, 10)))
        write_table("<append=%true>//tmp/t2", self._rows(*range(3)))
        write_table("<append=%true>//tmp/t2", self._rows(*range(3, 7)))
        write_table("<append=%true>//tmp/t2", self._rows(*range(7, 10)))

        merge(
            in_=["//tmp/t1", "//tmp/t2"],
            out="<create=%true>//tmp/t_out",
            spec={
                "omit_inaccessible_rows": True,
            },
            mode="sorted",
            authenticated_user="prime_user",
            merge_by=["int"],
        )

        assert sorted_dicts(read_table("//tmp/t_out")) == self._rows(2, 2, 3, 3, 5, 5, 7, 7)

    def test_reduce_simple(self, optimize_for):
        self._prepare_simple_test(optimize_for, sorted=True)
        reduce(
            in_="//tmp/t",
            out="<create=%true>//tmp/t_out",
            spec={
                "omit_inaccessible_rows": True,
                "format": "yson",
            },
            authenticated_user="prime_user",
            reducer_command="cat",
            reduce_by=["int"],
        )
        assert sorted_dicts(read_table("//tmp/t_out")) == self._rows(2, 3, 5, 7)

    def test_join_reduce_simple(self, optimize_for):
        self._create_users()
        self._create_table(optimize_for, sorted=True, path="//tmp/primary")
        self._create_table(optimize_for, sorted=True, path="//tmp/foreign")

        write_table("<append=%true>//tmp/primary", self._rows(*range(5)))
        write_table("<append=%true>//tmp/primary", self._rows(5, 5))
        write_table("<append=%true>//tmp/primary", self._rows(*range(6, 10)))

        for row in self._rows(2, 3, 4):
            write_table("<append=%true>//tmp/foreign", [row])
        write_table("<append=%true>//tmp/foreign", self._rows(5, 6, 7))

        join_reduce(
            in_=["//tmp/primary", "<foreign=%true>//tmp/foreign"],
            out=["<create=%true>//tmp/t_out_primary", "<create=%true>//tmp/t_out_foreign"],
            spec={
                "omit_inaccessible_rows": True,
                "format": "yson",
                "data_weight_per_job": 1,
            },
            authenticated_user="prime_user",
            reducer_command="cat",
            join_by=["int"],
        )
        assert sorted_dicts(read_table("//tmp/t_out_primary")) == self._rows(2, 3, 5, 5, 7)
        assert sorted_dicts(read_table("//tmp/t_out_foreign")) == self._rows(2, 3, 5, 7)

    def test_join_reduce_no_read(self, optimize_for):
        self._create_users()
        self._create_table(optimize_for, sorted=True, path="//tmp/primary")
        self._create_table(optimize_for, sorted=True, path="//tmp/foreign")

        write_table("<append=%true>//tmp/primary", self._rows(*range(10)))

        for row in self._rows(2, 3, 4, 5, 6, 7):
            write_table("<append=%true>//tmp/foreign", [row])

        join_reduce(
            in_=["//tmp/primary", "<foreign=%true>//tmp/foreign"],
            out=["<create=%true>//tmp/t_out_primary", "<create=%true>//tmp/t_out_foreign"],
            spec={
                "omit_inaccessible_rows": True,
                "format": "yson",
                "data_weight_per_job": 1,
            },
            authenticated_user="basic_read_user",
            reducer_command="cat",
            join_by=["int"],
        )
        assert sorted_dicts(read_table("//tmp/t_out_primary")) == []
        assert sorted_dicts(read_table("//tmp/t_out_foreign")) == []

    @pytest.mark.parametrize("order", ["unordered", "ordered"])
    def test_map_reduce_simple(self, optimize_for, order):
        self._prepare_simple_test(optimize_for)
        map_reduce(
            in_="//tmp/t",
            out="<create=%true>//tmp/t_out",
            spec={
                "omit_inaccessible_rows": True,
                "format": "yson",
            },
            authenticated_user="prime_user",
            mapper_command="cat",
            reducer_command="cat",
            ordered=order == "ordered",
            reduce_by=["str"],
        )
        assert sorted_dicts(read_table("//tmp/t_out")) == self._rows(2, 3, 5, 7)

    def test_sort_simple(self, optimize_for):
        self._prepare_simple_test(optimize_for, sorted=False)
        rows = self._rows(*range(10))
        Random(42).shuffle(rows)
        write_table("//tmp/t", rows)
        create(
            "table",
            "//tmp/t_out",
            attributes={
                "schema": [
                    make_sorted_column("int", "int64"),
                    make_column("str", "string"),
                ],
            },
        )
        sort(
            in_="//tmp/t",
            out="//tmp/t_out",
            sort_by="int",
            spec={
                "omit_inaccessible_rows": True,
            },
            authenticated_user="prime_user",
        )
        assert read_table("//tmp/t_out") == self._rows(2, 3, 5, 7)

    def test_rename_columns(self, optimize_for):
        self._prepare_simple_test(optimize_for)
        map(
            in_="<rename_columns={int=str;str=int}>//tmp/t",
            out="<create=%true>//tmp/t_out",
            spec={
                "omit_inaccessible_rows": True,
                "ordered": True,
                "format": "yson",
            },
            authenticated_user="prime_user",
            mapper_command="cat",
        )

        assert read_table("//tmp/t_out") == [
            {"int": row["str"], "str": row["int"]}
            for row in self._rows(2, 3, 5, 7)
        ]

    def test_full_read(self, optimize_for):
        self._prepare_simple_test(optimize_for)

        def do(start_op_partial, spec_patch={}, output_is_sorted=True):
            remove("//tmp/t_out", force=True)
            start_op_partial(
                in_="//tmp/t",
                out="<create=%true>//tmp/t_out",
                authenticated_user="full_read_user",
                spec={
                    "omit_inaccessible_rows": True,
                    "format": "yson",
                    **spec_patch,
                }
            )

            if output_is_sorted:
                assert read_table("//tmp/t_out") == self._rows(*range(10))
            else:
                assert sorted_dicts(read_table("//tmp/t_out")) == self._rows(*range(10))

        do(partial(map, command="cat", ordered=True))
        do(partial(map, command="cat", ordered=False), output_is_sorted=False)
        do(partial(map_reduce, mapper_command="cat", reducer_command="cat"), {"reduce_by": ["int"]}, output_is_sorted=False)
        do(partial(merge), {"mode": "unordered"}, output_is_sorted=False)
        do(partial(merge), {"mode": "ordered"})

        sort(in_="//tmp/t", out="//tmp/t", sort_by=["int"])

        do(partial(reduce, command="cat"), {"reduce_by": ["int"]}, output_is_sorted=False)
        do(partial(merge), {"mode": "sorted", "merge_by": ["int"]})

    def test_no_row_indices(self, optimize_for):
        if optimize_for == "lookup":
            pytest.skip()
        self._prepare_simple_test("scan")

        def run_merge(**kwargs):
            kwargs = {
                **kwargs,
                "mode": "unordered",
                "spec": {
                    "omit_inaccessible_rows": True,
                },
            }
            return merge(**kwargs)

        def assert_throws(in_):
            with raises_yt_error("Cannot use ranges with \"row_index\""):
                run_merge(in_=in_, out="<create=%true>//tmp/t_out", authenticated_user="prime_user")

        assert_throws("//tmp/t[#1]")
        assert_throws("//tmp/t[#1:#3]")
        assert_throws(["//tmp/t", "//tmp/t[#1]"])

        # No restrictions if we can prove that row indices are not broken.
        run_merge(in_="//tmp/t[#1]", out="<create=%true>//tmp/t_out", authenticated_user="full_read_user")

    def test_omit_rows_in_files(self, optimize_for):
        self._prepare_simple_test(optimize_for)
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"max": 42}])
        one_node = list(ls("//sys/exec_nodes"))[0]

        def run_map(user):
            map(
                in_="//tmp/t_in",
                out=[
                    "<create=%true>//tmp/t_out_input",
                    "<create=%true>//tmp/t_out_file",
                ],
                spec={
                    "omit_inaccessible_rows": True,
                    "mapper": {
                        "format": "yson",
                    },
                    "scheduling_tag_filter": one_node,
                },
                command="cat && cat ./t > /proc/self/fd/4",
                file=["<format=yson>//tmp/t"],
                authenticated_user=user,
            )

        run_map("prime_user")

        assert read_table("//tmp/t_out_input") == [{"max": 42}]
        assert sorted_dicts(read_table("//tmp/t_out_file")) == sorted_dicts(self._rows(2, 3, 5, 7))

        # Check that artifact is not cached with different rls parameters.
        run_map("basic_read_user")

        assert read_table("//tmp/t_out_input") == [{"max": 42}]
        assert read_table("//tmp/t_out_file") == []

    @authors("coteeq")
    @pytest.mark.parametrize("use_columns", [False, True], ids=["use_columns", "no_use_columns"])
    def test_key_widening(self, optimize_for, use_columns):
        self._prepare_simple_test(optimize_for, sorted=True)

        new_schema = [
            make_sorted_column("int", "int64"),
            make_sorted_column("int2", optional_type("int64")),
            make_column("str", "string"),
        ]
        alter_table("//tmp/t", schema=new_schema)

        reducer = dedent(
            """
            import json
            from sys import stdin

            control_attributes = {}
            for line in stdin:
                try:
                    row = json.loads(line)
                except Exception as ex:
                    raise RuntimeError(f"line was {line}") from ex
                if "$value" in row:
                    assert row["$value"] is None
                    control_attributes.update(row["$attributes"])
                    continue

                row.update({
                    key.removeprefix("$"): value
                    for key, value in control_attributes.items()
                })
                control_attributes = {}
                print(json.dumps(row))
            """
        )

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", reducer.encode("utf-8"))

        yt_set("//tmp/t/@acl", [
            self._make_rl_ace("prime_user", """str in ("val_5", "val_7")"""),
            self._make_rl_ace("prime_user", """str in ("val_2", "val_3")"""),
            make_ace("allow", "prime_user", "read"),
        ])

        path = "//tmp/t{str,int,int2}" if use_columns else "//tmp/t"
        op = reduce(
            track=False,
            in_=path,
            out="<create=%true>//tmp/t_out",
            spec={
                "omit_inaccessible_rows": True,
                "reducer": {"format": "json"},
            },
            authenticated_user="prime_user",
            command="python3 reducer.py",
            file="//tmp/reducer.py",
            reduce_by=["int", "int2"],
        )

        # This effectively asserts that operation will have inifinitely aborting jobs.
        wait(lambda: op.get_job_count("aborted") > 10)
        op.abort()
