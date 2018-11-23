import yt.environment.init_operation_archive as init_operation_archive
from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

from yt.common import YT_DATETIME_FORMAT_STRING

from operations_archive import clean_operations

import pytest

from time import sleep


def pytest_generate_tests(metafunc):
    if "read_from" in metafunc.fixturenames:
        metafunc.parametrize("read_from", metafunc.cls.read_from_values)


class ListOperationsSetup(YTEnvSetup):
    _input_path = "//testing/input"
    _output_path = "//testing/output"
    _archive_version = None # Latest

    @classmethod
    def _create_operation(cls, op_type, user, state=None, can_fail=False, pool_trees=None, title=None, abort=False, owners=None, **kwargs):
        if title is not None:
            set_branch(kwargs, ["spec", "title"], title)
        if owners is not None:
            set_branch(kwargs, ["spec", "owners"], owners)

        before_start_time = datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING)

        op = start_op(
            op_type,
            in_=cls._input_path,
            out=cls._output_path,
            dont_track=True,
            authenticated_user=user,
            **kwargs)

        if abort:
            op.abort()
        else:
            try:
                op.track()
            except YtError as err:
                if not can_fail or "Failed jobs limit exceeded" not in err.message:
                    raise

        op.before_start_time = before_start_time
        op.finish_time = get(op.get_path() + "/@finish_time")
        return op

    @classmethod
    def _create_operations(cls):
        cls.op1 = cls._create_operation("map", command="exit 0", user="user1")
        cls.op2 = cls._create_operation("map", command="exit 0", user="user2", owners=["group1", "user3"])
        cls.op3 = cls._create_operation(
            "map_reduce",
            mapper_command="exit 1",
            reducer_command="exit 0",
            user="user3",
            owners=["group2"],
            can_fail=True,
            sort_by="key",
            title="op3 title",
            spec={"max_failed_job_count": 2},
        )

        op4_spec = {
            "pool_trees": ["default", "other"],
            "scheduling_options_per_pool_tree": {
                "other": {"pool": "some_pool"},
            },
        }
        cls.op4 = cls._create_operation(
            "reduce",
            command="sleep 1000",
            user="user3",
            owners=["large_group"],
            reduce_by="key",
            abort=True,
            spec=op4_spec,
        )

        cls.op5 = cls._create_operation("sort", user="user4", sort_by="key")

        op6_spec = {
            "pool_trees": ["other"],
            "scheduling_options_per_pool_tree": {
                "other": {"pool": "pool_no_running"},
            },
        }
        before_start_time = datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING)
        cls.op6 = start_op("sort", in_=cls._input_path, out=cls._output_path, dont_track=True, authenticated_user="user5", spec=op6_spec, sort_by="key")
        cls.op6.before_start_time = before_start_time

    @classmethod
    def setup_class(cls):
        super(ListOperationsSetup, cls).setup_class()

        # Init operations archive.
        sync_create_cells(1)
        if cls._archive_version is None:
            init_operation_archive.create_tables_latest_version(cls.Env.create_native_client())
        else:
            init_operation_archive.create_tables(cls.Env.create_native_client(), cls._archive_version)

        create("table", cls._input_path, recursive=True, ignore_existing=True,
            attributes={
                "schema": [
                    {"name":"key", "type": "int64", "sort_order":"ascending"},
                    {"name":"value", "type": "int64"},
                ]
            }
        )

        write_table(cls._input_path, {"key": 1, "value": 2})

        create("table", cls._output_path, recursive=True, ignore_existing=True)

        # Setup pool trees.
        set("//sys/pool_trees/default/@nodes_filter", "!other")

        nodes = ls("//sys/nodes")
        set("//sys/nodes/" + nodes[0] + "/@user_tags/end", "other")

        create("map_node", "//sys/pool_trees/other", attributes={"nodes_filter": "tag"}, ignore_existing=True)
        create("map_node", "//sys/pool_trees/other/some_pool", ignore_existing=True)
        create("map_node", "//sys/pool_trees/other/pool_no_running", ignore_existing=True)
        set("//sys/pool_trees/other/pool_no_running/@max_running_operation_count", 0)

        wait(lambda: exists(
            "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{}/fair_share_info".format(pool_tree)))

        # Create users and groups.
        for i in range(1,6):
            create_user("user{0}".format(i))
        create_group("group1")
        create_group("group2")
        create_group("large_group")
        create_group("admins")
        set("//sys/operations/@acl/end", make_ace("allow", "admins", ["write"]))
        add_member("user1", "group1")
        add_member("user2", "group2")
        add_member("group1", "large_group")
        add_member("group2", "large_group")
        add_member("user4", "admins")

        set("//testing/@acl/end", make_ace("allow", "everyone", ["read", "write"]))

        cls._create_operations()


class _TestListOperationsBase(ListOperationsSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    SINGLE_SETUP_TEARDOWN = True

    # The following tests expect five operations to be present
    # in Cypress (and/or in the archive if |self.include_archive| is |True|):
    #     TYPE       -    STATE   - USER  -   POOLS            - FAILED JOBS - OWNERS
    #  1. map        - completed  - user1 -   user1            - False       -  []
    #  2. map        - completed  - user2 -   user2            - False       - [group1, user3]
    #  3. map_reduce - failed     - user3 -   user3            - True        - [group2]
    #  4. reduce     - aborted    - user3 - [user3, some_pool] - False       - [large_group]
    #  5. sort       - completed  - user4 -   user4            - False       -  []
    #  6. sort       - pending    - user5 -  pool_no_running   - False       -  []
    # Moreover, |self.op3| is expected to have title "op3 title".

    def test_invalid_arguments(self):
        # Should fail when limit is invalid.
        with pytest.raises(YtError):
            list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op1.finish_time, limit=99999)

        # Should fail when cursor_time is out of range (before |from_time|).
        with pytest.raises(YtError):
            list_operations(include_archive=self.include_archive, from_time=self.op2.before_start_time, to_time=self.op2.finish_time, cursor_time=self.op1.before_start_time)

        # Should fail when cursor_time is out of range (after |to_time|).
        with pytest.raises(YtError):
            list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op1.finish_time, cursor_time=self.op5.before_start_time)

    def test_time_filter(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, read_from=read_from)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1, "some_pool": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 3, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op4.id, self.op3.id, self.op2.id, self.op1.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, read_from=read_from)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id, self.op2.id, self.op1.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op2.before_start_time, to_time=self.op2.finish_time, read_from=read_from)
        assert res["pool_counts"] == {"user2": 1}
        assert res["user_counts"] == {"user2": 1}
        assert res["state_counts"] == {"completed": 1}
        assert res["type_counts"] == {"map": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [self.op2.id]

    def test_with_cursor(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, cursor_time=self.op2.finish_time, cursor_direction="past", read_from=read_from)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, cursor_time=self.op2.before_start_time, cursor_direction="future", read_from=read_from)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id, self.op2.id]

    def test_without_cursor_with_direction(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, cursor_direction="past", limit=2, read_from=read_from)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1, "some_pool": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 3, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op4.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, cursor_direction="future", limit=2, read_from=read_from)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1, "some_pool": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 3, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

    def test_type_filter(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op4.finish_time, type="map_reduce", read_from=read_from)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 2, "some_pool": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2}
        assert res["state_counts"] == {"completed": 2, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op4.finish_time, type="map", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op4.finish_time, type="reduce", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id]

    def test_state_filter(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, state="completed", read_from=read_from)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1, "some_pool": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 3, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 2, "sort": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op2.id, self.op1.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, state="failed", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, state="initializing", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == []

    def test_user_filter(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, user="user2", read_from=read_from)
        assert res["pool_counts"] == {"user3": 2, "user1": 1, "user2": 1, "user4": 1, "some_pool": 1}
        assert res["user_counts"] == {"user3": 2, "user1": 1, "user2": 1, "user4": 1}
        assert res["state_counts"] == {"completed": 1}
        assert res["type_counts"] == {"map": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [self.op2.id]

    def test_text_filter(self, read_from):
        # Title filter.
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, filter="op3 title", read_from=read_from)
        assert res["pool_counts"] == {"user3": 1}
        assert res["user_counts"] == {"user3": 1}
        assert res["state_counts"] == {"failed": 1}
        assert res["type_counts"] == {"map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        # Pool filter.
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, filter="some_pool", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id]

    def test_pool_filter(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op4.finish_time, pool="user3", read_from=read_from)
        assert res["pool_counts"] == {"user3": 2, "user1": 1, "user2": 1, "some_pool": 1}
        assert res["user_counts"] == {"user3": 2}
        assert res["state_counts"] == {"failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map_reduce": 1, "reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op4.finish_time, pool="some_pool", read_from=read_from)
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [self.op4.id]

    def test_with_limit(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, limit=1, read_from=read_from)
        assert res["pool_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["user_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["state_counts"] == {"failed": 1, "completed": 2}
        assert res["type_counts"] == {"map_reduce": 1, "map": 2}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]
        assert res["incomplete"] == True

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, limit=3, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op3.id, self.op2.id, self.op1.id]
        assert res["incomplete"] == False

    def test_has_failed_jobs(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, with_failed_jobs=True, read_from=read_from)
        assert res["pool_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["user_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["state_counts"] == {"failed": 1, "completed": 2}
        assert res["type_counts"] == {"map_reduce": 1, "map": 2}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, with_failed_jobs=False, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

    def test_attribute_filter(self, read_from):
        attributes = ["id", "start_time", "type", "brief_spec", "finish_time", "progress"]
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, attributes=attributes, read_from=read_from)
        assert res["pool_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["user_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["state_counts"] == {"failed": 1, "completed": 2}
        assert res["type_counts"] == {"map_reduce": 1, "map": 2}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id, self.op2.id, self.op1.id]
        assert all(sorted(op.keys()) == sorted(attributes) for op in res["operations"])

class TestListOperationsCypressOnly(_TestListOperationsBase):
    USE_DYNAMIC_TABLES = False

    include_archive = False
    read_from_values = ["cache", "follower"]
    check_failed_jobs_count = True

    def test_no_filters(self, read_from):
        res = list_operations(include_archive=self.include_archive)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1, "some_pool": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 3, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 1}
        assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op4.id, self.op3.id, self.op2.id, self.op1.id]

    def test_owned_by_filter(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, owned_by="user3", read_from=read_from)
        assert res["pool_counts"] == {"user2": 1, "user3": 2, "some_pool": 1}
        assert res["user_counts"] == {"user2": 1, "user3": 2}
        assert res["state_counts"] == {"completed": 1, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 1, "map_reduce": 1, "reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id, self.op2.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, owned_by="user1", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op2.id, self.op1.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, owned_by="user2", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id, self.op2.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, owned_by="user4", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op4.id, self.op3.id, self.op2.id, self.op1.id]

    def test_has_failed_jobs_with_pending(self, read_from):
        res = list_operations(include_archive=self.include_archive, with_failed_jobs=True, read_from=read_from)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1, "some_pool": 1, "pool_no_running": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1, "user5": 1}
        # NB(levysotsky): "pending" state is currently mapped to "running" in list_operations.
        assert res["state_counts"] == {"completed": 3, "failed": 1, "aborted": 1, "running": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 2}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        res = list_operations(include_archive=self.include_archive, with_failed_jobs=False, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op6.id, self.op5.id, self.op4.id, self.op2.id, self.op1.id]


class TestListOperationsCypressArchive(_TestListOperationsBase):
    USE_DYNAMIC_TABLES = True

    include_archive = True
    read_from_values=["follower"]
    check_failed_jobs_count = False

    def test_time_range_missing(self):
        with pytest.raises(YtError):
            list_operations(include_archive=True, to_time=self.op5.finish_time)
        with pytest.raises(YtError):
            list_operations(include_archive=True, from_time=self.op1.before_start_time)

    def test_owned_by_filter_is_not_supported(self):
        with pytest.raises(YtError):
            list_operations(include_archive=True, owned_by="root")


class TestListOperationsArchiveOnly(_TestListOperationsBase):
    USE_DYNAMIC_TABLES = True

    include_archive = True
    read_from_values=["follower"]
    check_failed_jobs_count = False

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_cleaner": {
                "enable": True,
                # Analyze all operations each 100ms
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
            "static_orchid_cache_update_period": 100,
            "alerts_update_period": 100
        }
    }

    @classmethod
    def setup_class(cls):
        super(TestListOperationsArchiveOnly, cls).setup_class()

        def has_operations():
            for entry in ls("//sys/operations"):
                if len(entry) != 2:
                    return True
                if len(ls("//sys/operations/" + entry)) != 0:
                    return True
            return False

        wait(lambda: not has_operations())

    def test_owned_by_filter_is_not_supported(self):
        with pytest.raises(YtError):
            list_operations(include_archive=True, owned_by="root")
