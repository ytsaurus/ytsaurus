import yt.environment.init_operation_archive as init_operation_archive
from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

from yt.common import YT_DATETIME_FORMAT_STRING, uuid_to_parts

import pytest


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
    _input_path = "//testing/input"
    _output_path = "//testing/output"
    _archive_version = None # Latest

    @classmethod
    def _create_operation(cls, op_type, user, state=None, can_fail=False, pool_trees=None, title=None, abort=False, owners=None, annotations=None, **kwargs):
        if title is not None:
            set_branch(kwargs, ["spec", "title"], title)
        if owners is not None:
            set_branch(kwargs, ["spec", "owners"], owners)
        if annotations is not None:
            set_branch(kwargs, ["spec", "annotations"], annotations)

        before_start_time = datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING)

        op = start_op(
            op_type,
            in_=cls._input_path,
            out=cls._output_path,
            track=False,
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
        cls.op1 = cls._create_operation("map", command="exit 0", user="user1", annotations={"key": ["annotation1", "annotation2"]})
        cls.op2 = cls._create_operation("map", command="exit 0", user="user2", owners=["group1", "user3"])
        cls.op3 = cls._create_operation(
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
                    make_ace("deny", "user6", ["manage"])
                ],
            },
        )

        cls.op4 = cls._create_operation(
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

        cls.op5 = cls._create_operation("sort", user="user4", sort_by="key")

    @classmethod
    def setup_class(cls):
        super(ListOperationsSetup, cls).setup_class()

        # Init operations archive.
        sync_create_cells(1)
        if cls._archive_version is None:
            init_operation_archive.create_tables_latest_version(cls.Env.create_native_client(), override_tablet_cell_bundle="default")
        else:
            init_operation_archive.create_tables(cls.Env.create_native_client(), cls._archive_version, override_tablet_cell_bundle="default")

        create("table", cls._input_path, recursive=True, ignore_existing=True,
            attributes={
                "schema": [
                    {"name":"key", "type": "int64", "sort_order":"ascending"},
                    {"name":"value", "type": "int64"},
                ],
                "dynamic": cls.DRIVER_BACKEND == "rpc"
            }
        )

        if cls.DRIVER_BACKEND != "rpc":
            write_table(cls._input_path, {"key": 1, "value": 2})
        else:
            sync_mount_table(cls._input_path)
            insert_rows(cls._input_path, [{"key": 1, "value": 2}])
            sync_unmount_table(cls._input_path)

        create("table", cls._output_path, recursive=True, ignore_existing=True)

        # Setup pool trees.
        set("//sys/pool_trees/default/@nodes_filter", "!other")

        nodes = ls("//sys/cluster_nodes")
        set("//sys/cluster_nodes/" + nodes[0] + "/@user_tags/end", "other")

        create_pool_tree("other", attributes={"nodes_filter": "tag"}, ignore_existing=True)
        create_pool("some_pool", pool_tree="other", ignore_existing=True)
        create_pool("pool_no_running", pool_tree="other", ignore_existing=True)
        set("//sys/pool_trees/other/pool_no_running/@max_running_operation_count", 0)

        # Create users and groups.
        for i in range(1,7):
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
        add_member("user6", "group2")

        set("//testing/@acl/end", make_ace("allow", "everyone", ["read", "write"]))

        def base_operation_acl_has_admins():
            for ace in get("//sys/scheduler/orchid/scheduler/operation_base_acl"):
                if "admins" in ace["subjects"]:
                    return True
            return False
        wait(base_operation_acl_has_admins)

        cls._create_operations()


class _TestListOperationsBase(ListOperationsSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    SINGLE_SETUP_TEARDOWN = True

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

    @authors("halin-george", "levysotsky")
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

    @authors("levysotsky")
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

    @authors("levysotsky")
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

    @authors("levysotsky")
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

    @authors("levysotsky")
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

    @authors("levysotsky")
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

    @authors("renadeen", "levysotsky")
    def test_user_filter(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, user="user3", read_from=read_from)
        assert res["pool_counts"] == {"user3": 2, "some_pool": 1}
        assert res["user_counts"] == {"user3": 2, "user1": 1, "user2": 1, "user4": 1}
        assert res["state_counts"] == {'failed': 1, 'aborted': 1}
        assert res["type_counts"] == {'map_reduce': 1L, 'reduce': 1L}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id]

    @authors("levysotsky")
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

        # Annotations filter.
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, filter="notation2", read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op1.id]

    @authors("levysotsky")
    def test_pool_filter(self, read_from):
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op4.finish_time, pool="user3", read_from=read_from)
        assert res["user_counts"] == {"user3": 2, "user1": 1, "user2": 1}
        assert res["pool_counts"] == {"user3": 2, "user1": 1, "user2": 1, "some_pool": 1}
        assert res["state_counts"] == {"failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map_reduce": 1, "reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id]

        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op4.finish_time, pool="some_pool", read_from=read_from)
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 0
        assert [op["id"] for op in res["operations"]] == [self.op4.id]

    @authors("levysotsky")
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

    @authors("levysotsky")
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

    @authors("levysotsky")
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

    @authors("levysotsky")
    def test_access_filter(self, read_from):
        access = {"subject": "user3", "permissions": ["read", "manage"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert res["pool_counts"] == {"user2": 1, "user3": 2, "some_pool": 1}
        assert res["user_counts"] == {"user2": 1, "user3": 2}
        assert res["state_counts"] == {"completed": 1, "failed": 1, "aborted": 1}
        assert res["type_counts"] == {"map": 1, "map_reduce": 1, "reduce": 1}
        if self.check_failed_jobs_count:
            assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id, self.op2.id]

        access = {"subject": "user1", "permissions": ["read"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id, self.op2.id, self.op1.id]

        access = {"subject": "user1", "permissions": ["read", "manage"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op2.id, self.op1.id]

        access = {"subject": "user2", "permissions": ["read"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id, self.op2.id]

        access = {"subject": "user2", "permissions": ["read", "manage"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id, self.op2.id]

        # user6 is like user2 with the only difference that he is banned from managing op3 and he is
        # not an authenticated user for op2.
        access = {"subject": "user6", "permissions": ["read"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id]

        access = {"subject": "user6", "permissions": ["read", "manage"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id]

        # user4 is admin.
        access = {"subject": "user4", "permissions": ["read"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op4.id, self.op3.id, self.op2.id, self.op1.id]

        access = {"subject": "user4", "permissions": ["read", "manage"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op4.id, self.op3.id, self.op2.id, self.op1.id]

        access = {"subject": "group1", "permissions": ["read"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id, self.op2.id]

        access = {"subject": "group1", "permissions": ["read", "manage"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op2.id]

        access = {"subject": "large_group", "permissions": ["read"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id, self.op3.id]

        access = {"subject": "large_group", "permissions": ["read", "manage"]}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op4.id]

        # Anyone has '[]' permissions for any operation.
        access = {"subject": "large_group", "permissions": []}
        res = list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)
        assert [op["id"] for op in res["operations"]] == [self.op5.id, self.op4.id, self.op3.id, self.op2.id, self.op1.id]

    @authors("levysotsky")
    def test_access_filter_errors(self, read_from):
        # Missing subject.
        access = {"permissions": ["read", "manage"]}
        with pytest.raises(YtError):
            list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)

        # Missing permissions.
        access = {"subject": "user1"}
        with pytest.raises(YtError):
            list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)

        access = {"subject": "unknown_subject", "permissions": ["read", "manage"]}
        with pytest.raises(YtError):
            list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)

        access = {"subject": "user1", "permissions": ["unknown_permission"]}
        with pytest.raises(YtError):
            list_operations(include_archive=self.include_archive, from_time=self.op1.before_start_time, to_time=self.op5.finish_time, access=access, read_from=read_from)


class TestListOperationsCypressOnly(_TestListOperationsBase):
    USE_DYNAMIC_TABLES = False

    include_archive = False
    read_from_values = ["cache", "follower"]
    check_failed_jobs_count = True

    @classmethod
    def setup_class(cls):
        super(TestListOperationsCypressOnly, cls).setup_class()
        op6_spec = {
            "pool_trees": ["other"],
            "scheduling_options_per_pool_tree": {
                "other": {"pool": "pool_no_running"},
            },
        }
        before_start_time = datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING)
        cls.op6 = start_op("sort", in_=cls._input_path, out=cls._output_path, track=False, authenticated_user="user5", spec=op6_spec, sort_by="key")
        cls.op6.before_start_time = before_start_time
        wait(lambda: get(cls.op6.get_path() + "/@state") == "pending")

    @authors("levysotsky")
    def test_no_filters(self, read_from):
        res = list_operations(include_archive=self.include_archive)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1, "some_pool": 1, "pool_no_running": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1, "user5": 1}
        assert res["state_counts"] == {"completed": 3, "failed": 1, "aborted": 1, "pending": 1}
        assert res["type_counts"] == {"map": 2, "map_reduce": 1, "reduce": 1, "sort": 2}
        assert res["failed_jobs_count"] == 1
        assert [op["id"] for op in res["operations"]] == [self.op6.id, self.op5.id, self.op4.id, self.op3.id, self.op2.id, self.op1.id]

    @authors("levysotsky")
    def test_has_failed_jobs_with_pending(self, read_from):
        res = list_operations(include_archive=self.include_archive, with_failed_jobs=True, read_from=read_from)
        assert res["pool_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1, "some_pool": 1, "pool_no_running": 1}
        assert res["user_counts"] == {"user1": 1, "user2": 1, "user3": 2, "user4": 1, "user5": 1}
        assert res["state_counts"] == {"completed": 3, "failed": 1, "aborted": 1, "pending": 1}
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

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "alerts_update_period": 100,
            "watchers_update_period": 100,
        }
    }

    @authors("levysotsky")
    def test_time_range_missing(self):
        with pytest.raises(YtError):
            list_operations(include_archive=True, to_time=self.op5.finish_time)
        with pytest.raises(YtError):
            list_operations(include_archive=True, from_time=self.op1.before_start_time)


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
            "alerts_update_period": 100,
            "watchers_update_period": 100,
        },
    }

    @classmethod
    def setup_class(cls):
        super(TestListOperationsArchiveOnly, cls).setup_class()
        wait(lambda: not operation_nodes_exist())


class TestListOperationsArchiveHacks(ListOperationsSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    SINGLE_SETUP_TEARDOWN = True

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
            "alerts_update_period": 100,
            "watchers_update_period": 100,
        },
    }

    @classmethod
    def setup_class(cls):
        super(TestListOperationsArchiveHacks, cls).setup_class()
        wait(lambda: not operation_nodes_exist())

    @authors("ilpauzner")
    def test_list_operation_sorting(self):
        clear_start_time("//sys/operations_archive/ordered_by_start_time")
        clear_start_time("//sys/operations_archive/ordered_by_id")
        ops = list_operations(include_archive=True,
                              from_time=datetime.utcfromtimestamp(0).strftime(YT_DATETIME_FORMAT_STRING),
                              to_time=datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING))["operations"]
        ids = [uuid_to_parts(op["id"]) for op in ops]
        assert ids == sorted(ids, reverse=True)

    @authors("kiselyovp")
    def test_archive_fetching(self):
        clear_progress("//sys/operations_archive/ordered_by_id")
        wait(lambda: select_rows("brief_progress FROM [//sys/operations_archive/ordered_by_id]")[0]["brief_progress"] == {"ivan": "ivanov"})
        res = list_operations(include_archive=False)
        for op in res["operations"]:
            assert op["brief_progress"] == {"ivan": "ivanov"}


##################################################################

class TestListOperationsCypressOnlyRpcProxy(TestListOperationsCypressOnly):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


class TestListOperationsCypressArchiveRpcProxy(TestListOperationsCypressArchive):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


class TestListOperationsArchiveOnlyRpcProxy(TestListOperationsArchiveOnly):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
