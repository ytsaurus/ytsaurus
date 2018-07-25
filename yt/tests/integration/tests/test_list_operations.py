import yt.environment.init_operation_archive as init_operation_archive
from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

from yt.common import YT_DATETIME_FORMAT_STRING

from operations_archive import clean_operations

import pytest

def pytest_generate_tests(metafunc):
    if "read_from" in metafunc.fixturenames:
        metafunc.parametrize("read_from", metafunc.cls.read_from_values)

class _TestListOperationsBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    _input_path = "//testing/input"
    _output_path = "//testing/output"

    def _create_operation(self, op_type, user, state=None, can_fail=False, pool_trees=None, title=None, abort=False, **kwargs):
        if pool_trees:
            set_branch(kwargs, ["spec", "pool_trees"], pool_trees)
        else:
            set_branch(kwargs, ["spec", "pool"], user)

        if title:
            set_branch(kwargs, ["spec", "title"], title)

        before_start_time = datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING)
        op = start_op(
            op_type,
            in_=self._input_path,
            out=self._output_path,
            dont_track=True,
            authenticated_user=user,
            **kwargs)

        if abort:
            op.abort()
        else:
            try:
                op.track()
            except YtError as err:
                print("Error: {0}".format(err))
                assert can_fail
                assert "Failed jobs limit exceeded" in err.message

        if state:
            set(op.get_path() + "/@state", state)
        op.before_start_time = before_start_time
        op.finish_time = get(op.get_path() + "/@finish_time")
        return op

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())
        create("table", self._input_path, recursive=True, ignore_existing=True)
        write_table(self._input_path, {"key": 1, "value": 2})
        create("table", self._output_path, recursive=True, ignore_existing=True)

        # Create a new pool tree.
        tag = "other"
        create("map_node", "//sys/pool_trees/other", ignore_existing=True)
        set("//sys/pool_trees/other/@nodes_filter", tag)
        set("//sys/pool_trees/default/@nodes_filter", "!" + tag)

        node = ls("//sys/nodes")[0]
        set("//sys/nodes/" + node + "/@user_tags/end", tag)

        wait(lambda: exists("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/other/fair_share_info"))

        # Create users.
        for i in range(1,5):
            create_user("user{0}".format(i))
        set("//testing/@acl/end", make_ace("allow", "everyone", ["read", "write"]))

        self._create_operations()


    # This following tests expect five operations to be present
    # in Cypress (and/or in the archive if |self.include_archive| is |True|):
    #     TYPE       -    STATE     - USER  -   POOL    - HAS FAILED JOBS
    #  1. map        - completed    - user1 -   user1   - False
    #  2. map        - completed    - user2 -   user2   - False
    #  3. map_reduce - failed       - user3 -   user3   - True
    #  4. reduce     - aborted      - user3 - <unknown> - False
    #  5. sort       - initializing - user4 -   user4   - False
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
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1, "user4": 1, "unknown": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        # XXX(levysotsky): "initializing" and many other states are collapsed into "running"
        # in filters and counters. Maybe it should be fixed eventually.
        assert res["state_counts"] == {"completed": 2, "failed": 1, "aborted": 1, "running": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op4.id, self.op3.id, self.op2.id, self.op1.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, read_from=read_from)
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id, self.op2.id, self.op1.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op2.before_start_time, to_time=self.op2.finish_time, read_from=read_from)
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user2": 1}
        assert res["user_counts"] == {"user2": 1}
        assert res["state_counts"] == {"completed": 1}
        assert res["type_counts"] == {"map": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [self.op2.id]

    def test_with_cursor(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, cursor_time=self.op2.finish_time, cursor_direction="past", read_from=read_from)
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, cursor_time=self.op2.before_start_time, cursor_direction="future", read_from=read_from)
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id, self.op2.id]

    def test_without_cursor_with_direction(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, cursor_direction="past", limit=2, read_from=read_from)
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1, "user4": 1, "unknown": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1, "aborted": 1, "running": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op4.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, cursor_direction="future", limit=2, read_from=read_from)
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1, "user4": 1, "unknown": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1, "aborted": 1, "running": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

    def test_type_filter(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op4.finish_time, type="map_reduce", read_from=read_from)
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user3": 1, "user1": 1, "user2": 1, "unknown": 1}
        assert res["user_counts"] == {"user3": 2, "user1": 1, "user2": 1}
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
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1, "user4": 1, "unknown": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1, "aborted": 1, "running": 1}
        assert res["type_counts"] == {"map": 2}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, state="failed", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, state="initializing", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == []

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, state="running", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op5.id]

    def test_user_filter(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, user="user2", read_from=read_from)
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user3": 1, "user1": 1, "user2": 1, "user4": 1, "unknown": 1}
        assert res["user_counts"] == {"user3": 2, "user1": 1, "user2": 1, "user4": 1}
        assert res["state_counts"] == {"completed": 1}
        assert res["type_counts"] == {"map": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [self.op2.id]

    def test_text_filter(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, filter="op3 title", read_from=read_from)
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user3": 1}
        assert res["user_counts"] == {"user3": 1}
        assert res["state_counts"] == {"failed": 1}
        assert res["type_counts"] == {"map_reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

    # TODO(levysotsky): Uncomment this test when pools are fixed.
    #def test_pool_filter(self, read_from):
    #    res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op4.finish_time, pool="user3", read_from=read_from)
    #    assert res["pool_counts"] == {"user3": 1, "user1": 1, "user2": 1, "unknown": 1}
    #    assert res["user_counts"] == {"user3": 2}
    #    assert res["state_counts"] == {"failed": 1, "aborted": 1}
    #    assert res["type_counts"] == {"map_reduce": 1, "reduce": 1}
    #    if self.check_failed_jobs_count:
    #        assert res["failed_jobs_count"] == 1
    #    assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id]

    def test_with_limit(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, limit=1, read_from=read_from)
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user3": 1, "user1": 1, "user2": 1}
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
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["user_counts"] == {"user3": 1, "user1": 1, "user2": 1}
        assert res["state_counts"] == {"failed": 1, "completed": 2}
        assert res["type_counts"] == {"map_reduce": 1, "map": 2}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op3.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, with_failed_jobs=False, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op2.id, self.op1.id]

    # TODO(levysotsky): Uncomment when attribute filters are added.
    #def test_attribute_filter(self, read_from):
    #    attributes = ["id", "type", "brief_spec", "finish_time"]
    #    res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op3.finish_time, attributes=attributes, read_from=read_from)
    #    #assert res["pool_counts"] == {"user3": 1, "user1": 1, "user2": 1}
    #    assert res["user_counts"] == {"user3": 1, "user1": 1, "user2": 1}
    #    assert res["state_counts"] == {"failed": 1, "completed": 2}
    #    assert res["type_counts"] == {"map_reduce": 1, "map": 2}
    #    if self.check_failed_jobs_count:
    #        assert res["failed_jobs_count"] == 1
    #    assert [op["id"] for op in res["operations"]] == [self.op3.id, self.op2.id, self.op1.id]
    #    assert all(sorted(op.keys()) == sorted(attributes) for op in res["operations"])

class TestListOperationCypressOnly(_TestListOperationsBase):
    include_archive = False
    read_from_values = ["cache", "follower"]
    check_failed_jobs_count = True

    def _create_operations(self):
        self.op1 = self._create_operation("map", command="exit 0", user="user1")
        self.op2 = self._create_operation("map", command="exit 0", user="user2")
        self.op3 = self._create_operation(
            "map_reduce",
            mapper_command="exit 1",
            reducer_command="exit 0",
            user="user3",
            can_fail=True,
            sort_by="key",
            title="op3 title",
            spec={"max_failed_job_count": 2})
        self.op4 = self._create_operation("reduce", command="sleep 10", user="user3", reduce_by="key", abort=True, pool_trees=["other"])
        self.op5 = self._create_operation("sort", user="user4", state="initializing", sort_by="key")

    def test_no_filters(self, read_from):
        res = list_operations(include_archive=False)
        # TODO(levysotsky): Uncomment pool checks when pools are fixed.
        #assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 1, "user4": 1, "unknown": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1}
        assert res["state_counts"] == {"completed": 2, "failed": 1, "aborted": 1, "running": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 1}
        assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op4.id, self.op3.id, self.op2.id, self.op1.id]

class TestListOperationsCypressArchive(_TestListOperationsBase):
    include_archive = True
    read_from_values=["follower"]
    check_failed_jobs_count = False

    def _create_operations(self):
        self.op1 = self._create_operation("map", command="exit 0", user="user1")
        self.op2 = self._create_operation("map", command="exit 0", user="user2")
        self.op3 = self._create_operation(
            "map_reduce",
            mapper_command="exit 1",
            reducer_command="exit 0",
            user="user3",
            can_fail=True,
            sort_by="key",
            title="op3 title",
            spec={"max_failed_job_count": 2})

        clean_operations(client=self.Env.create_native_client())

        self.op4 = self._create_operation("reduce", command="sleep 10", user="user3", reduce_by="key", abort=True, pool_trees=["other"])
        self.op5 = self._create_operation("sort", user="user4", state="initializing", sort_by="key")

    def test_time_range_missing(self):
        with pytest.raises(YtError):
            list_operations(include_archive=True, to_time=self.op5.finish_time)

        with pytest.raises(YtError):
            list_operations(include_archive=True, from_time=self.op1.before_start_time)
