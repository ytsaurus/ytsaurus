from yt_commands import (
    authors, wait, wait_no_assert,
    wait_breakpoint, release_breakpoint, with_breakpoint, create, get, set,
    exists, create_user,
    create_group, make_ace, add_member, read_table, write_table, map, map_reduce, abort_job, abandon_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec, dump_job_context,
    poll_job_shell, abort_op,
    complete_op, suspend_op,
    resume_op, clean_operations, sync_create_cells,
    update_controller_agent_config, update_op_parameters, raises_yt_error)

from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
)

from yt_scheduler_helpers import scheduler_orchid_path

import yt_error_codes

import yt.environment.init_operation_archive as init_operation_archive

from yt.common import YtError

import pytest

import random
import string
import time
from contextlib import contextmanager
from copy import deepcopy

##################################################################


def _abort_op(**kwargs):
    abort_op(kwargs.pop("operation_id"), **kwargs)


class TestSchedulerAcls(YTEnvSetup):
    NUM_TEST_PARTITIONS = 3
    USE_DYNAMIC_TABLES = True

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_poll_job_shell": True,
            },
        },
    }

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
            "operations_update_period": 100,
        },
        "controller_agent": {
            "watchers_update_period": 100,
        },
    }

    operation_authenticated_user = "operation_authenticated_user"
    no_rights_user = "no_rights_user"
    read_only_user = "read_only_user"
    manage_only_user = "manage_only_user"
    manage_and_read_group = "manage_and_read_group"
    manage_and_read_user = "manage_and_read_user"
    banned_from_managing_user = "banned_from_managing_user"
    group_membership = {manage_and_read_group: [manage_and_read_user, banned_from_managing_user]}

    spec = {
        "acl": [
            make_ace("allow", read_only_user, "read"),
            make_ace("allow", manage_only_user, "manage"),
            make_ace("allow", manage_and_read_group, ["manage", "read"]),
            make_ace("deny", banned_from_managing_user, ["manage"]),
        ],
    }

    def setup_method(self, method):
        super(TestSchedulerAcls, self).setup_method(method)

        # Init operations archive.
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )

        for user in [
            self.operation_authenticated_user,
            self.no_rights_user,
            self.read_only_user,
            self.manage_only_user,
            self.manage_and_read_user,
            self.banned_from_managing_user,
        ]:
            create_user(user)

        for group in [self.manage_and_read_group]:
            create_group(group)

        for group, members in self.group_membership.items():
            for member in members:
                add_member(member, group)

    @staticmethod
    def _random_string(length):
        return "".join(random.choice(string.ascii_letters) for _ in range(length))

    def _create_tables(self):
        input_path = "//tmp/input_" + self._random_string(5)
        output_path = "//tmp/output_" + self._random_string(5)
        create("table", input_path)
        write_table(input_path, {"key": i for i in range(20)})
        create("table", output_path)
        return input_path, output_path

    @staticmethod
    def _validate_access(user, should_have_access, action, **action_args):
        has_access = True
        authorization_error = None
        try:
            action(authenticated_user=user, **action_args)
        except YtError as e:
            if not e.contains_code(yt_error_codes.AuthorizationErrorCode):
                raise
            authorization_error = e
            has_access = False
        if has_access != should_have_access:
            message = (
                'User "{user}" should {maybe_not}have permission to perform '
                'action "{action}" with arguments {action_args}'.format(
                    user=user,
                    maybe_not="" if should_have_access else "not ",
                    action=action.__name__,
                    action_args=action_args,
                )
            )
            if not has_access:
                message += ". Got error response {}".format(authorization_error)
            raise AssertionError(message)

    def _run_and_fail_op(self, should_update_operation_parameters):
        input_path, output_path = self._create_tables()
        breakpoint_name = "breakpoint_" + self._random_string(10)
        spec = deepcopy(self.spec)
        spec["job_count"] = 1
        spec["max_failed_job_count"] = 1
        if should_update_operation_parameters:
            del spec["acl"]
        op = map(
            track=False,
            in_=input_path,
            out=output_path,
            command=with_breakpoint(
                "cat; echo SOME-STDERR >&2; BREAKPOINT; exit 1",
                breakpoint_name=breakpoint_name,
            ),
            authenticated_user=self.operation_authenticated_user,
            spec=spec,
        )
        (job_id,) = wait_breakpoint(breakpoint_name=breakpoint_name)
        if should_update_operation_parameters:
            update_op_parameters(op.id, parameters={"acl": self.spec["acl"]})
        release_breakpoint(breakpoint_name=breakpoint_name)
        with pytest.raises(YtError):
            op.track()
        return op, job_id

    @contextmanager
    def _run_op_context_manager(self, should_update_operation_parameters=False, spec=None):
        input_path, output_path = self._create_tables()
        breakpoint_name = "breakpoint_" + self._random_string(10)
        command = with_breakpoint("echo SOME-STDERR >&2; cat; BREAKPOINT;", breakpoint_name=breakpoint_name)
        if spec is None:
            spec = deepcopy(self.spec)
        if should_update_operation_parameters:
            saved_acl = spec.pop("acl")
        op = map(
            track=False,
            in_=input_path,
            out=output_path,
            command=command,
            authenticated_user=self.operation_authenticated_user,
            spec=spec,
        )
        try:
            (job_id,) = wait_breakpoint(breakpoint_name=breakpoint_name)
            if should_update_operation_parameters:
                update_op_parameters(op.id, parameters={"acl": saved_acl})
            wait(op.get_running_jobs)

            yield op, job_id

            release_breakpoint(breakpoint_name=breakpoint_name)
        finally:
            with raises_yt_error(yt_error_codes.Scheduler.NoSuchOperation, required=False):
                op.complete()
            try:
                op.track()
            except YtError:
                # TODO: Ensure it is "no such operation" error or operation has failed or aborted.
                pass

    @authors("levysotsky")
    @pytest.mark.parametrize("should_update_operation_parameters", [False, True])
    def test_read_job_from_node_actions(self, should_update_operation_parameters):
        def _dump_job_context(operation_id, job_id, **kwargs):
            dump_job_context(job_id, "//tmp/job_context", **kwargs)

        actions = [
            _dump_job_context,
            get_job_input,
            get_job_stderr,
            get_job_spec,
        ]

        with self._run_op_context_manager(should_update_operation_parameters) as (
            op,
            job_id,
        ):
            for action in actions:
                self._validate_access(
                    self.no_rights_user,
                    False,
                    action,
                    operation_id=op.id,
                    job_id=job_id,
                )
                self._validate_access(self.read_only_user, True, action, operation_id=op.id, job_id=job_id)
                self._validate_access(
                    self.manage_only_user,
                    False,
                    action,
                    operation_id=op.id,
                    job_id=job_id,
                )
                self._validate_access(
                    self.manage_and_read_user,
                    True,
                    action,
                    operation_id=op.id,
                    job_id=job_id,
                )
                self._validate_access(
                    self.banned_from_managing_user,
                    True,
                    action,
                    operation_id=op.id,
                    job_id=job_id,
                )

    @authors("levysotsky")
    @pytest.mark.parametrize("should_update_operation_parameters", [False, True])
    def test_read_job_from_cypress_actions(self, should_update_operation_parameters):
        actions = [
            get_job_fail_context,
            get_job_stderr,
        ]

        op, job_id = self._run_and_fail_op(should_update_operation_parameters)
        for action in actions:
            self._validate_access(self.no_rights_user, False, action, operation_id=op.id, job_id=job_id)
            self._validate_access(self.read_only_user, True, action, operation_id=op.id, job_id=job_id)
            self._validate_access(self.manage_only_user, False, action, operation_id=op.id, job_id=job_id)
            self._validate_access(
                self.manage_and_read_user,
                True,
                action,
                operation_id=op.id,
                job_id=job_id,
            )
            self._validate_access(
                self.banned_from_managing_user,
                True,
                action,
                operation_id=op.id,
                job_id=job_id,
            )

    @authors("levysotsky")
    def test_read_job_from_archive_actions(self):
        actions = [
            get_job_fail_context,
            get_job_input,
            get_job_stderr,
            get_job_spec,
        ]

        op, job_id = self._run_and_fail_op(should_update_operation_parameters=False)
        clean_operations()
        for action in actions:
            self._validate_access(self.no_rights_user, False, action, operation_id=op.id, job_id=job_id)
            self._validate_access(self.read_only_user, True, action, operation_id=op.id, job_id=job_id)
            self._validate_access(self.manage_only_user, False, action, operation_id=op.id, job_id=job_id)
            self._validate_access(
                self.manage_and_read_user,
                True,
                action,
                operation_id=op.id,
                job_id=job_id,
            )
            self._validate_access(
                self.banned_from_managing_user,
                True,
                action,
                operation_id=op.id,
                job_id=job_id,
            )

    @authors("levysotsky")
    @pytest.mark.parametrize("should_update_operation_parameters", [False, True])
    def test_manage_job_actions(self, should_update_operation_parameters):
        actions = [
            abort_job,
            abandon_job,
        ]

        for action in actions:
            with self._run_op_context_manager(should_update_operation_parameters) as (
                op,
                job_id,
            ):
                self._validate_access(self.no_rights_user, False, action, job_id=job_id)
                self._validate_access(self.read_only_user, False, action, job_id=job_id)
                self._validate_access(self.manage_only_user, True, action, job_id=job_id)
            with self._run_op_context_manager(should_update_operation_parameters) as (
                op,
                job_id,
            ):
                self._validate_access(self.manage_and_read_user, True, action, job_id=job_id)

    @authors("levysotsky")
    @pytest.mark.parametrize("should_update_operation_parameters", [False, True])
    def test_manage_and_read_job_actions(self, should_update_operation_parameters):
        def spawn_job_shell(operation_id, job_id, **kwargs):
            poll_job_shell(
                job_id,
                kwargs.pop("authenticated_user"),
                operation="spawn",
                term="screen-256color",
                height=50,
                width=132,
            )

        actions = [
            spawn_job_shell,
        ]

        for action in actions:
            with self._run_op_context_manager(should_update_operation_parameters) as (
                op,
                job_id,
            ):
                self._validate_access(
                    self.no_rights_user,
                    False,
                    action,
                    operation_id=op.id,
                    job_id=job_id,
                )
                self._validate_access(
                    self.read_only_user,
                    False,
                    action,
                    operation_id=op.id,
                    job_id=job_id,
                )
                self._validate_access(
                    self.manage_only_user,
                    False,
                    action,
                    operation_id=op.id,
                    job_id=job_id,
                )
                self._validate_access(
                    self.manage_and_read_user,
                    True,
                    action,
                    operation_id=op.id,
                    job_id=job_id,
                )
                self._validate_access(
                    self.banned_from_managing_user,
                    False,
                    action,
                    operation_id=op.id,
                    job_id=job_id,
                )

    @authors("levysotsky")
    @pytest.mark.parametrize("should_update_operation_parameters", [False, True])
    def test_manage_operation_actions(self, should_update_operation_parameters):
        def _complete_op(**kwargs):
            complete_op(kwargs.pop("operation_id"), **kwargs)

        def suspend_and_resume_op(**kwargs):
            operation_id = kwargs.pop("operation_id")
            suspend_op(operation_id, **kwargs)
            resume_op(operation_id, **kwargs)

        def _update_op_parameters(**kwargs):
            kwargs["parameters"] = {"scheduling_options_per_pool_tree": {"default": {"weight": 3.0}}}
            update_op_parameters(kwargs.pop("operation_id"), **kwargs)

        actions = [
            _abort_op,
            _complete_op,
            suspend_and_resume_op,
            _update_op_parameters,
        ]

        for action in actions:
            with self._run_op_context_manager(should_update_operation_parameters) as (
                op,
                _,
            ):
                self._validate_access(self.no_rights_user, False, action, operation_id=op.id)
                self._validate_access(self.read_only_user, False, action, operation_id=op.id)
                self._validate_access(self.banned_from_managing_user, False, action, operation_id=op.id)
                self._validate_access(self.manage_only_user, True, action, operation_id=op.id)
            with self._run_op_context_manager(should_update_operation_parameters) as (
                op,
                _,
            ):
                self._validate_access(self.manage_and_read_user, True, action, operation_id=op.id)

    @authors("levysotsky")
    def test_scheduler_operation_abort_by_owners(self):
        spec = {"owners": [self.manage_and_read_user]}
        with self._run_op_context_manager(spec=spec) as (op, job_id):
            self._validate_access(self.no_rights_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.read_only_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.manage_only_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.banned_from_managing_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.manage_and_read_user, True, _abort_op, operation_id=op.id)

    @authors("levysotsky")
    def test_acl_priority_over_owners(self):
        spec = {
            "owners": [self.no_rights_user],
            "acl": [make_ace("allow", self.manage_and_read_user, "manage")],
        }
        with self._run_op_context_manager(spec=spec) as (op, job_id):
            wait(lambda: list(op.get_alerts().keys()) == ["owners_in_spec_ignored"])
            self._validate_access(self.no_rights_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.read_only_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.manage_only_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.banned_from_managing_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.manage_and_read_user, True, _abort_op, operation_id=op.id)

    @authors("levysotsky")
    def test_invalid_acl(self):
        spec = {
            "acl": [
                {
                    # Note the typo in "permissions".
                    "permission": ["read", "manage"],
                    "subjects": [self.manage_and_read_user],
                    "action": "allow",
                }
            ],
        }
        with pytest.raises(YtError):
            with self._run_op_context_manager(spec=spec) as (op, job_id):
                pass

    @authors("levysotsky")
    def test_acl_errors(self):
        # Wrong permissions.
        with pytest.raises(YtError):
            with self._run_op_context_manager(
                spec={
                    "acl": [make_ace("allow", self.manage_and_read_user, ["read", "write"])],
                }
            ):
                pass

    @authors("levysotsky")
    def test_acl_update_errors(self):
        with self._run_op_context_manager() as (op, job_id):
            # Wrong permissions.
            with pytest.raises(YtError):
                update_op_parameters(
                    op.id,
                    parameters={"acl": [make_ace("allow", self.manage_and_read_user, ["read", "write"])]},
                )

            # Missing user.
            update_op_parameters(
                op.id,
                parameters={"acl": [make_ace("allow", "missing_user", ["read", "manage"])]},
            )
            wait(lambda: op.get_alerts())
            assert list(op.get_alerts().keys()) == ["invalid_acl"]

            with Restarter(self.Env, SCHEDULERS_SERVICE):
                pass
            time.sleep(0.1)
            op.wait_for_state("running")

            assert list(op.get_alerts().keys()) == ["invalid_acl"]

            update_op_parameters(op.id, parameters={"acl": []})
            wait(lambda: not op.get_alerts())

    @authors("levysotsky")
    @pytest.mark.parametrize("allow_access", [False, True])
    def test_allow_users_group_access_to_intermediate_data(self, allow_access):
        update_controller_agent_config("allow_users_group_read_intermediate_data", allow_access)

        input_path, output_path = self._create_tables()
        breakpoint_name = "breakpoint_" + self._random_string(10)
        op = map_reduce(
            track=False,
            mapper_command="cat",
            reducer_command=with_breakpoint("cat; BREAKPOINT", breakpoint_name=breakpoint_name),
            in_=input_path,
            out=output_path,
            sort_by=["key"],
            spec={
                "acl": [make_ace("allow", self.manage_and_read_user, ["read", "manage"])],
            },
        )

        wait_breakpoint(breakpoint_name=breakpoint_name)

        @wait_no_assert
        def transaction_and_intermediate_exist():
            assert exists(op.get_path() + "/@async_scheduler_transaction_id")
            scheduler_transaction_id = get(op.get_path() + "/@async_scheduler_transaction_id")
            assert exists(op.get_path() + "/intermediate", tx=scheduler_transaction_id)

        scheduler_transaction_id = get(op.get_path() + "/@async_scheduler_transaction_id")
        if allow_access:
            read_table(
                op.get_path() + "/intermediate",
                tx=scheduler_transaction_id,
                authenticated_user=self.no_rights_user,
            )
        else:
            with raises_yt_error(yt_error_codes.AuthorizationErrorCode):
                read_table(
                    op.get_path() + "/intermediate",
                    tx=scheduler_transaction_id,
                    authenticated_user=self.no_rights_user,
                )

        release_breakpoint(breakpoint_name=breakpoint_name)
        op.track()

    @authors("levysotsky")
    @pytest.mark.parametrize("add_authenticated_user", [False, True])
    def test_add_authenticated_user_to_acl(self, add_authenticated_user):
        spec = {
            "acl": [make_ace("allow", self.manage_and_read_user, ["manage", "read"])],
            "add_authenticated_user_to_acl": add_authenticated_user,
        }
        with self._run_op_context_manager(spec=spec) as (op, job_id):
            self._validate_access(
                self.operation_authenticated_user,
                add_authenticated_user,
                _abort_op,
                operation_id=op.id,
            )

    @authors("eshcherbin")
    def test_revive_base_acl_with_write(self):
        assert not get("//sys/operations/@acl")
        set("//sys/operations/@acl/end", make_ace("allow", "users", ["write"]))
        wait(lambda: [ace for ace in get(scheduler_orchid_path() + "/scheduler/operation_base_acl") if "users" in ace["subjects"]])

        with self._run_op_context_manager() as (op, job_id):
            with Restarter(self.Env, SCHEDULERS_SERVICE):
                pass
            op.wait_for_state("running")

        set("//sys/operations/@acl", [])
