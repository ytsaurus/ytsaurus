from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, wait_no_assert, set_branch, create, ls, get, set, create_user,
    create_group, create_pool, create_pool_tree,
    make_ace, add_member, insert_rows, select_rows, write_table, start_op,
    list_operations, clean_operations, sync_create_cells, sync_mount_table,
    sync_unmount_table, make_random_string)

import yt.environment.init_operations_archive as init_operations_archive

from yt.common import YT_DATETIME_FORMAT_STRING, uuid_to_parts, YtError

import pytest

from datetime import datetime


def pytest_generate_tests(metafunc):
    if "read_from" in metafunc.fixturenames:
        metafunc.parametrize("read_from", metafunc.cls.read_from_values)


def clear_start_time(path):
    ops = select_rows("id_hi, id_lo, start_time, state FROM [{}]".format(path))
    for i in range(len(ops)):
        ops[i]["start_time"] = 0
    insert_rows(path, ops, update=True)


def clear_progress(path):
    ops = select_rows("id_hi, id_lo, brief_progress, progress FROM [{}]".format(path))
    for i in range(len(ops)):
        ops[i]["brief_progress"] = {"ivan": "ivanov"}
        ops[i]["progress"] = {"semen": "semenych", "semenych": "gorbunkov"}
    insert_rows(path, ops, update=True)


class ListOperationsSetup(YTEnvSetup):
    _input_path = "//tmp/testing/input"

    @staticmethod
    def _create_output_path():
        path = "//tmp/testing/" + make_random_string()
        create("table", path, recursive=True, ignore_existing=True)
        return path

    def _create_operation(
        self,
        op_type,
        user,
        state=None,
        can_fail=False,
        pool_trees=None,
        title=None,
        abort=False,
        owners=None,
        annotations=None,
        **kwargs
    ):
        if title is not None:
            set_branch(kwargs, ["spec", "title"], title)
        if owners is not None:
            set_branch(kwargs, ["spec", "owners"], owners)
        if annotations is not None:
            set_branch(kwargs, ["spec", "annotations"], annotations)

        before_start_time = datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING)

        output_path = self._create_output_path()
        op = start_op(op_type, in_=self._input_path, out=output_path, track=False, authenticated_user=user, **kwargs)

        op.after_start_time = datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING)

        def my_track():
            if abort:
                op.abort()
            else:
                try:
                    op.track()
                except YtError as err:
                    if not can_fail or "Failed jobs limit exceeded" not in err.message:
                        raise

        op.my_track = my_track
        op.before_start_time = before_start_time
        return op

    def _create_operations(self):
        self.before_all_operations = datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING)
        self.op1 = self._create_operation(
            "map",
            command="exit 0",
            user="user1",
            annotations={"key": ["annotation1", "annotation2"]},
        )
        self.op2 = self._create_operation("map", command="exit 0", user="user2", owners=["group1", "user3"])
        self.op3 = self._create_operation(
            "map_reduce",
            mapper_command="exit 1",
            reducer_command="exit 0",
            user="user3",
            can_fail=True,
            sort_by="key",
            title="op3 title",
            spec={
                "max_failed_job_count": 2,
                "acl": [
                    make_ace("allow", "group2", ["manage", "read"]),
                    make_ace("allow", "large_group", "read"),
                    make_ace("deny", "user6", ["manage"]),
                ],
            },
        )

        self.op4 = self._create_operation(
            "reduce",
            command="sleep 1000",
            user="user3",
            owners=["large_group"],
            reduce_by="key",
            abort=True,
            spec={
                "pool_trees": ["default", "other"],
                "scheduling_options_per_pool_tree": {
                    "other": {"pool": "some_pool"},
                },
            },
        )

        self.op5 = self._create_operation("sort", user="user4", sort_by="key")

        for op in [self.op1, self.op2, self.op3, self.op4, self.op5]:
            op.my_track()

    def setup_method(self, method):
        super(ListOperationsSetup, self).setup_method(method)

        if self.include_archive:
            sync_create_cells(1)
            init_operations_archive.create_tables_latest_version(
                self.Env.create_native_client(),
                override_tablet_cell_bundle="default",
            )

        create(
            "table",
            self._input_path,
            recursive=True,
            ignore_existing=True,
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"},
                ],
                "dynamic": self.DRIVER_BACKEND == "rpc",
            },
        )

        if self.DRIVER_BACKEND != "rpc":
            write_table(self._input_path, {"key": 1, "value": 2})
        else:
            sync_create_cells(1)
            sync_mount_table(self._input_path)
            insert_rows(self._input_path, [{"key": 1, "value": 2}])
            sync_unmount_table(self._input_path)

        # Setup pool trees.
        set("//sys/pool_trees/default/@config/node_tag_filter", "!other")

        nodes = ls("//sys/cluster_nodes")
        set("//sys/cluster_nodes/" + nodes[0] + "/@user_tags/end", "other")

        create_pool_tree("other", config={"node_tag_filter": "tag"}, ignore_existing=True)
        create_pool("some_pool", pool_tree="other", ignore_existing=True)
        create_pool("pool_no_running", pool_tree="other", ignore_existing=True)
        set("//sys/pool_trees/other/pool_no_running/@max_running_operation_count", 0)

        # Create users and groups.
        for i in range(1, 7):
            create_user("user{0}".format(i))
        create_group("group1")
        create_group("group2")
        create_group("large_group")
        set("//sys/operations/@acl/end", make_ace("allow", "admins", ["write"]))
        add_member("user1", "group1")
        add_member("user2", "group2")
        add_member("group1", "large_group")
        add_member("group2", "large_group")
        add_member("user4", "admins")
        add_member("user6", "group2")

        set("//tmp/testing/@acl/end", make_ace("allow", "everyone", ["read", "write"]))

        @wait_no_assert
        def base_operation_acl_has_admins():
            assert any(
                "admins" in ace["subjects"]
                for ace in get("//sys/scheduler/orchid/scheduler/operation_base_acl")
            )

        self._create_operations()


@pytest.mark.enabled_multidaemon
class _TestListOperationsBase(ListOperationsSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    # The following tests expect the following operations to be present
    # in Cypress and/or in the archive (depending on |self.include_archive|):
    #     TYPE       -    STATE   - USER  -   POOLS          - FAILED JOBS - READ_ACCESS   - MANAGE_ACCESS   - ANNOTATIONS
    #  1. map        - completed  - user1 -  user1           - False       - []            - []              - {key=[annotation1;annotation2]}
    #  2. map        - completed  - user2 -  user2           - False       - [group1,      - [group1, user3] - {}
    #                                                                          user3]
    #  3. map_reduce - failed     - user3 -  user3           - True        - [group2,      - [group2         - {}
    #                                                                         large_group]    except user6]
    #  4. reduce     - aborted    - user3 - [user3,          - False       - [large_group] - [large_group]   - {}
    #                                        some_pool]
    #  5. sort       - completed  - user4 -  user4           - False       - []            - []              - {}
    #  6. sort       - pending    - user5 -  pool_no_running - False       - []            - []              - {}
    # Moreover, |self.op3| is expected to have title "op3 title".

    @authors("omgronny")
    def test_invalid_arguments(self):
        # Should fail when limit is invalid.
        with pytest.raises(YtError):
            list_operations(
                include_archive=self.include_archive,
                from_time=self.op1.before_start_time,
                to_time=self.op1.after_start_time,
                limit=99999,
            )

        # Should fail when cursor_time is out of range (before |from_time|).
        with pytest.raises(YtError):
            list_operations(
                include_archive=self.include_archive,
                from_time=self.op2.before_start_time,
                to_time=self.op2.after_start_time,
                cursor_time=self.op1.before_start_time,
            )

        # Should fail when cursor_time is out of range (after |to_time|).
        with pytest.raises(YtError):
            list_operations(
                include_archive=self.include_archive,
                from_time=self.op1.before_start_time,
                to_time=self.op1.after_start_time,
                cursor_time=self.before_all_operations,
            )

    @authors("omgronny")
    def test_time_filter(self, read_from):
        self._test_time_ranges(read_from)
        self._test_with_cursor(read_from)
        self._test_with_direction(read_from)

    def _test_time_ranges(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            read_from=read_from,
        )
        assert res["pool_counts"] == {
            "user1": 1,
            "user2": 1,
            "user3": 2,
            "user4": 1,
            "some_pool": 1,
        }
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 3, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [
            self.op5.id,
            self.op4.id,
            self.op3.id,
            self.op2.id,
            self.op1.id,
        ]

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op3.after_start_time,
            read_from=read_from,
        )
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [
            self.op3.id,
            self.op2.id,
            self.op1.id,
        ]

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op2.before_start_time,
            to_time=self.op2.after_start_time,
            read_from=read_from,
        )
        assert res["pool_counts"] == {"user2": 1}
        assert res["user_counts"] == {"user2": 1}
        assert res["state_counts"] == {"completed": 1}
        assert res["type_counts"] == {"map": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [self.op2.id]

    def _test_with_cursor(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op3.after_start_time,
            cursor_time=self.op2.after_start_time,
            cursor_direction="past",
            read_from=read_from,
        )
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op3.after_start_time,
            cursor_time=self.op2.before_start_time,
            cursor_direction="future",
            read_from=read_from,
        )
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id, self.op2.id]

    def _test_with_direction(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            cursor_direction="past",
            limit=2,
            read_from=read_from,
        )
        assert res["pool_counts"] == {
            "user1": 1,
            "user2": 1,
            "user3": 2,
            "user4": 1,
            "some_pool": 1,
        }
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 3, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op4.id]

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            cursor_direction="future",
            limit=2,
            read_from=read_from,
        )
        assert res["pool_counts"] == {
            "user1": 1,
            "user2": 1,
            "user3": 2,
            "user4": 1,
            "some_pool": 1,
        }
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 3, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

    @authors("omgronny")
    def test_filters(self, read_from):
        self._test_type_filter(read_from)
        self._test_state_filter(read_from)
        self._test_user_filter(read_from)
        self._test_text_filter(read_from)
        self._test_pool_filter(read_from)
        self._test_has_failed_jobs(read_from)
        self._test_pool_tree_filter(read_from)
        self._test_all_filters(read_from)

    def _test_type_filter(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op4.after_start_time,
            type="map_reduce",
            read_from=read_from,
        )
        assert res["pool_counts"] == {"user3": 1}
        assert res["user_counts"] == {"user3": 1}
        assert res["state_counts"] == {"failed": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op4.after_start_time,
            type="map",
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op4.after_start_time,
            type="reduce",
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [self.op4.id]

    def _test_state_filter(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            state="completed",
            read_from=read_from,
        )
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user4": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user4": 1}
        assert res["state_counts"] == {"completed": 3, "aborted": 1, "failed": 1}
        assert res["type_counts"] == {"map": 2, "sort": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [
            self.op5.id,
            self.op2.id,
            self.op1.id,
        ]

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            state="failed",
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            state="initializing",
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == []

    def _test_user_filter(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            user="user3",
            read_from=read_from,
        )
        assert res["pool_counts"] == {"user3": 2, "some_pool": 1}
        assert res["user_counts"] == {"user3": 2, "user1": 1, "user2": 1, "user4": 1}
        assert res["state_counts"] == {"failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map_reduce": 1, "reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id]

    def _test_text_filter(self, read_from):
        # Title filter.
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            filter="op3 title",
            read_from=read_from,
        )
        assert res["pool_counts"] == {"user3": 1}
        assert res["user_counts"] == {"user3": 1}
        assert res["state_counts"] == {"failed": 1}
        assert res["type_counts"] == {"map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        # Pool filter.
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            filter="some_pool",
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [self.op4.id]

        # Annotations filter.
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            filter="notation2",
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [self.op1.id]
        assert [op["runtime_parameters"]["annotations"] for op in res["operations"]] == [{"key": ["annotation1", "annotation2"]}]

    def _test_pool_filter(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op4.after_start_time,
            pool="user3",
            read_from=read_from,
        )
        assert res["user_counts"] == {"user3": 2}
        assert res["pool_counts"] == {
            "user3": 2,
            "user1": 1,
            "user2": 1,
            "some_pool": 1,
        }
        assert res["state_counts"] == {"failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map_reduce": 1, "reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id]

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op4.after_start_time,
            pool="some_pool",
            read_from=read_from,
        )
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [self.op4.id]

    def _test_has_failed_jobs(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op3.after_start_time,
            with_failed_jobs=True,
            read_from=read_from,
        )
        assert res["user_counts"] == {"user3": 1}
        assert res["pool_counts"] == {"user3": 1}
        assert res["state_counts"] == {"failed": 1}
        assert res["type_counts"] == {"map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op3.after_start_time,
            with_failed_jobs=False,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

    def _test_pool_tree_filter(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            read_from=read_from,
            pool_tree="other",
        )
        assert res["user_counts"] == {"user3": 1}
        assert res["pool_counts"] == {
            "user3": 1,
            "some_pool": 1,
        }
        assert res["state_counts"] == {"aborted": 1}
        assert res["type_counts"] == {"reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [self.op4.id]
        assert res["pool_tree_counts"] == {
            "default": 5,
            "other": 1,
        }
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            read_from=read_from,
            pool_tree="default",
            pool="some_pool",
        )
        assert res.get("user_counts", {}) == {}
        assert res["pool_tree_counts"] == {"other": 1}
        assert res.get("pool_counts", {}) == {}
        assert res.get("state_counts", {}) == {}
        assert res.get("type_counts", {}) == {}
        if self.check_failed_jobs_count:
            assert res.get("failed_jobs_count", 0) == 0
        assert res.get("operations", []) == []

    def _test_all_filters(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op4.after_start_time,
            pool="user3",
            type="reduce",
            read_from=read_from,
        )

        assert res["pool_counts"] == {"user3": 1, "some_pool": 1}
        assert res["user_counts"] == {"user3": 1}
        assert res["state_counts"] == {"aborted": 1}
        assert res["type_counts"] == {"map_reduce": 1, "reduce": 1}

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op4.after_start_time,
            state="aborted",
            type="map",
            read_from=read_from,
        )

        assert res.get("pool_counts", {}) == {}
        assert res.get("user_counts", {}) == {}
        assert res["state_counts"] == {"completed": 2}
        assert res["type_counts"] == {"reduce": 1}

    @authors("omgronny")
    def test_with_limit(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op3.after_start_time,
            limit=1,
            read_from=read_from,
        )
        assert res["pool_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["user_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["state_counts"] == {"failed": 1, "completed": 2}
        assert res["type_counts"] == {"map_reduce": 1, "map": 2}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]
        assert res["incomplete"]

        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op3.after_start_time,
            limit=3,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [
            self.op3.id,
            self.op2.id,
            self.op1.id,
        ]
        assert not res["incomplete"]

    @authors("omgronny")
    def test_attribute_filter(self, read_from):
        attributes = [
            "id",
            "start_time",
            "type",
            "brief_spec",
            "provided_spec",
            "finish_time",
            "progress",
        ]
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op3.after_start_time,
            attributes=attributes,
            read_from=read_from,
        )
        assert res["pool_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["user_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["state_counts"] == {"failed": 1, "completed": 2}
        assert res["type_counts"] == {"map_reduce": 1, "map": 2}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [
            self.op3.id,
            self.op2.id,
            self.op1.id,
        ]
        assert all(sorted(op.keys()) == sorted(attributes) for op in res["operations"])

    @authors("omgronny")
    def test_access_filter(self, read_from):
        access = {"subject": "user3", "permissions": ["read", "manage"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert res["pool_counts"] == {"user2": 1, "user3": 2, "some_pool": 1}
        assert res["user_counts"] == {"user2": 1, "user3": 2}
        assert res["state_counts"] == {"completed": 1, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 1, "map_reduce": 1, "reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [
            self.op4.id,
            self.op3.id,
            self.op2.id,
        ]

        access = {"subject": "user1", "permissions": ["read"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [
            self.op4.id,
            self.op3.id,
            self.op2.id,
            self.op1.id,
        ]

        access = {"subject": "user1", "permissions": ["read", "manage"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [
            self.op4.id,
            self.op2.id,
            self.op1.id,
        ]

        access = {"subject": "user2", "permissions": ["read"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [
            self.op4.id,
            self.op3.id,
            self.op2.id,
        ]

        access = {"subject": "user2", "permissions": ["read", "manage"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [
            self.op4.id,
            self.op3.id,
            self.op2.id,
        ]

        # user6 is like user2 with the only difference that he is banned from managing op3 and he is
        # not an authenticated user for op2.
        access = {"subject": "user6", "permissions": ["read"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id]

        access = {"subject": "user6", "permissions": ["read", "manage"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [self.op4.id]

        # user4 is admin.
        access = {"subject": "user4", "permissions": ["read"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [
            self.op5.id,
            self.op4.id,
            self.op3.id,
            self.op2.id,
            self.op1.id,
        ]

        access = {"subject": "user4", "permissions": ["read", "manage"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [
            self.op5.id,
            self.op4.id,
            self.op3.id,
            self.op2.id,
            self.op1.id,
        ]

        access = {"subject": "group1", "permissions": ["read"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [
            self.op4.id,
            self.op3.id,
            self.op2.id,
        ]

        access = {"subject": "group1", "permissions": ["read", "manage"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op2.id]

        access = {"subject": "large_group", "permissions": ["read"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id]

        access = {"subject": "large_group", "permissions": ["read", "manage"]}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [self.op4.id]

        # Anyone has '[]' permissions for any operation.
        access = {"subject": "large_group", "permissions": []}
        res = list_operations(
            include_archive=self.include_archive,
            from_time=self.op1.before_start_time,
            to_time=self.op5.after_start_time,
            access=access,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [
            self.op5.id,
            self.op4.id,
            self.op3.id,
            self.op2.id,
            self.op1.id,
        ]

        # Missing subject.
        access = {"permissions": ["read", "manage"]}
        with pytest.raises(YtError):
            list_operations(
                include_archive=self.include_archive,
                from_time=self.op1.before_start_time,
                to_time=self.op5.after_start_time,
                access=access,
                read_from=read_from,
            )

        # Missing permissions.
        access = {"subject": "user1"}
        with pytest.raises(YtError):
            list_operations(
                include_archive=self.include_archive,
                from_time=self.op1.before_start_time,
                to_time=self.op5.after_start_time,
                access=access,
                read_from=read_from,
            )

        access = {"subject": "unknown_subject", "permissions": ["read", "manage"]}
        with pytest.raises(YtError):
            list_operations(
                include_archive=self.include_archive,
                from_time=self.op1.before_start_time,
                to_time=self.op5.after_start_time,
                access=access,
                read_from=read_from,
            )

        access = {"subject": "user1", "permissions": ["unknown_permission"]}
        with pytest.raises(YtError):
            list_operations(
                include_archive=self.include_archive,
                from_time=self.op1.before_start_time,
                to_time=self.op5.after_start_time,
                access=access,
                read_from=read_from,
            )


@pytest.mark.enabled_multidaemon
class TestListOperationsCypressOnly(_TestListOperationsBase):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 4
    include_archive = False
    read_from_values = ["cache", "follower"]
    check_failed_jobs_count = True

    def setup_method(self, method):
        super(TestListOperationsCypressOnly, self).setup_method(method)
        op6_spec = {
            "pool_trees": ["other"],
            "scheduling_options_per_pool_tree": {
                "other": {"pool": "pool_no_running"},
            },
        }
        before_start_time = datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING)
        self.op6 = start_op(
            "sort",
            in_=self._input_path,
            out=self._create_output_path(),
            track=False,
            authenticated_user="user5",
            spec=op6_spec,
            sort_by="key",
        )
        self.op6.before_start_time = before_start_time
        wait(lambda: get(self.op6.get_path() + "/@state") == "pending")

    @authors("omgronny")
    def test_no_filters(self, read_from):
        res = list_operations(include_archive=self.include_archive)
        assert res["pool_counts"] == {
            "user1": 1,
            "user2": 1,
            "user3": 2,
            "user4": 1,
            "some_pool": 1,
            "pool_no_running": 1,
        }
        assert res["user_counts"] == {
            "user1": 1,
            "user2": 1,
            "user3": 2,
            "user4": 1,
            "user5": 1,
        }
        assert res["state_counts"] == {
            "completed": 3,
            "failed": 1,
            "aborted": 1,
            "pending": 1,
        }
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 2}
        assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [
            self.op6.id,
            self.op5.id,
            self.op4.id,
            self.op3.id,
            self.op2.id,
            self.op1.id,
        ]

    @authors("omgronny")
    def test_has_failed_jobs_with_pending(self, read_from):
        res = list_operations(
            include_archive=self.include_archive,
            with_failed_jobs=True,
            read_from=read_from,
        )
        assert res["user_counts"] == {"user3": 1}
        assert res["pool_counts"] == {"user3": 1}
        assert res["state_counts"] == {"failed": 1}
        assert res["type_counts"] == {"map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        res = list_operations(
            include_archive=self.include_archive,
            with_failed_jobs=False,
            read_from=read_from,
        )
        assert [op["id"] for op in res["operations"]] == [
            self.op6.id,
            self.op5.id,
            self.op4.id,
            self.op2.id,
            self.op1.id,
        ]


@pytest.mark.enabled_multidaemon
class TestListOperationsCypressArchive(_TestListOperationsBase):
    ENABLE_MULTIDAEMON = True
    USE_DYNAMIC_TABLES = True

    include_archive = True
    read_from_values = ["follower"]
    check_failed_jobs_count = False

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "alerts_update_period": 100,
            "watchers_update_period": 100,
        }
    }

    @authors("omgronny")
    def test_time_range_missing(self):
        with pytest.raises(YtError):
            list_operations(include_archive=True, to_time=self.op5.after_start_time)
        with pytest.raises(YtError):
            list_operations(include_archive=True, from_time=self.op1.before_start_time)


@pytest.mark.enabled_multidaemon
class TestListOperationsArchiveOnly(_TestListOperationsBase):
    ENABLE_MULTIDAEMON = True
    USE_DYNAMIC_TABLES = True

    include_archive = True
    read_from_values = ["follower"]
    check_failed_jobs_count = False

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_cleaner": {
                "enable": False,
                # Analyze all operations each 100ms
                "analysis_period": 100,
                # Wait each batch to remove not more than 100ms
                "remove_batch_timeout": 100,
                # Wait each batch to archive not more than 100ms
                "archive_batch_timeout": 100,
                # Retry sleeps
                "min_archivation_retry_sleep_delay": 100,
                "max_archivation_retry_sleep_delay": 110,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
            "fair_share_update_period": 100,
            "static_orchid_cache_update_period": 100,
            "alerts_update_period": 100,
            "watchers_update_period": 100,
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "scheduler_connector": {
                    "heartbeat_executor": {
                        "period": 100,  # 100 msec
                    },
                },
            }
        }
    }

    def setup_method(self, method):
        super(TestListOperationsArchiveOnly, self).setup_method(method)
        clean_operations()


@pytest.mark.enabled_multidaemon
class TestListOperationsArchiveHacks(ListOperationsSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    USE_DYNAMIC_TABLES = True

    include_archive = True
    read_from_values = ["follower"]
    check_failed_jobs_count = False

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_cleaner": {
                "enable": False,
                # Analyze all operations each 100ms
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
            "static_orchid_cache_update_period": 100,
            "alerts_update_period": 100,
            "watchers_update_period": 100,
        },
    }

    def setup_method(self, method):
        super(TestListOperationsArchiveHacks, self).setup_method(method)
        clean_operations()

    @authors("omgronny")
    def test_list_operation_sorting(self):
        clear_start_time("//sys/operations_archive/ordered_by_start_time")
        clear_start_time("//sys/operations_archive/ordered_by_id")
        ops = list_operations(
            include_archive=True,
            from_time=datetime.utcfromtimestamp(0).strftime(YT_DATETIME_FORMAT_STRING),
            to_time=datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING),
        )["operations"]
        ids = [uuid_to_parts(op["id"]) for op in ops]
        assert ids == sorted(ids, reverse=True)

    @authors("kiselyovp")
    def test_archive_fetching(self):
        clear_progress("//sys/operations_archive/ordered_by_id")
        wait(
            lambda: select_rows("brief_progress FROM [//sys/operations_archive/ordered_by_id]")[0]["brief_progress"]
            == {"ivan": "ivanov"}
        )
        res = list_operations(include_archive=False)
        for op in res["operations"]:
            assert op["brief_progress"] == {"ivan": "ivanov"}


##################################################################


@pytest.mark.enabled_multidaemon
class TestListOperationsCypressOnlyRpcProxy(TestListOperationsCypressOnly):
    ENABLE_MULTIDAEMON = True
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


@pytest.mark.enabled_multidaemon
class TestListOperationsCypressArchiveRpcProxy(TestListOperationsCypressArchive):
    ENABLE_MULTIDAEMON = True
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


@pytest.mark.enabled_multidaemon
class TestListOperationsArchiveOnlyRpcProxy(TestListOperationsArchiveOnly):
    ENABLE_MULTIDAEMON = True
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


@pytest.mark.enabled_multidaemon
class TestListOperationsCypressArchiveHeavyRuntimeParameters(TestListOperationsCypressArchive):
    ENABLE_MULTIDAEMON = True
    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "alerts_update_period": 100,
            "watchers_update_period": 100,
            "enable_heavy_runtime_parameters": True,
        }
    }


@authors("renadeen")
@pytest.mark.enabled_multidaemon
class TestArchiveVersion(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    def test_basic(self):
        assert init_operations_archive.get_latest_version() == get("//sys/scheduler/orchid/scheduler/config/min_required_archive_version")
