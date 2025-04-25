from yt_commands import (
    authors, wait, retry, wait_no_assert,
    wait_breakpoint, release_breakpoint, with_breakpoint, create, get, set,
    exists, create_user,
    create_group, make_ace, add_member, read_table, write_table, map, map_reduce, run_test_vanilla, abort_job, abandon_job,
    get_operation, get_job_fail_context, get_job_input, get_job_stderr, get_job_spec, dump_job_context,
    get_job, list_operations, list_jobs,
    poll_job_shell, abort_op,
    complete_op, suspend_op,
    resume_op, clean_operations, sync_create_cells, create_test_tables,
    update_controller_agent_config, update_op_parameters, update_access_control_object_acl, raises_yt_error,
    create_access_control_object_namespace, create_access_control_object)

from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
)

from yt_scheduler_helpers import scheduler_orchid_path

import yt_error_codes

import yt.environment.init_operations_archive as init_operations_archive

from yt.common import YtError, YT_DATETIME_FORMAT_STRING

import pytest

import random
import string
import time
from datetime import datetime
from contextlib import contextmanager
from copy import deepcopy

##################################################################


def _abort_op(**kwargs):
    abort_op(kwargs.pop("operation_id"), **kwargs)


def _complete_op(**kwargs):
    complete_op(kwargs.pop("operation_id"), **kwargs)


def suspend_and_resume_op(**kwargs):
    operation_id = kwargs.pop("operation_id")
    suspend_op(operation_id, **kwargs)
    resume_op(operation_id, **kwargs)


def _update_op_parameters(**kwargs):
    kwargs["parameters"] = {"scheduling_options_per_pool_tree": {"default": {"weight": 3.0}}}
    update_op_parameters(kwargs.pop("operation_id"), **kwargs)


class TestSchedulerAcls(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 6
    USE_DYNAMIC_TABLES = True

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_poll_job_shell": True,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_reporter": {
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                },
            },
        },
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_cleaner": {
                "enable": False,
                # Analyze all operations each 100ms
                "analysis_period": 100,
                # Wait each batch to archive not more than 100ms
                "archive_batch_timeout": 100,
                # Retry sleeps
                "min_archivation_retry_sleep_delay": 100,
                "max_archivation_retry_sleep_delay": 110,
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
    banned_user = "banned_user"
    group_membership = {manage_and_read_group: [manage_and_read_user, banned_from_managing_user]}

    spec = {
        "acl": [
            make_ace("allow", read_only_user, "read"),
            make_ace("allow", manage_only_user, "manage"),
            make_ace("allow", manage_and_read_group, ["manage", "read"]),
            make_ace("deny", banned_from_managing_user, ["manage"]),
            make_ace("deny", banned_user, ["manage", "read"]),
        ],
    }

    def setup_method(self, method):
        super(TestSchedulerAcls, self).setup_method(method)

        # Init operations archive.
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
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
            self.banned_user,
        ]:
            create_user(user)

        for group in [self.manage_and_read_group]:
            create_group(group)

        for group, members in self.group_membership.items():
            for member in members:
                add_member(member, group)

        create_access_control_object_namespace("operations")
        create_access_control_object("users", "operations")
        create_access_control_object("new_aco", "operations")
        update_access_control_object_acl("operations", "users", self.spec["acl"])

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

    @authors("omgronny")
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

    @authors("omgronny")
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

    @authors("omgronny")
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

    @authors("omgronny")
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

    @authors("omgronny")
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

    @authors("omgronny")
    @pytest.mark.parametrize("should_update_operation_parameters", [False, True])
    def test_manage_operation_actions(self, should_update_operation_parameters):
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

    @authors("omgronny")
    def test_scheduler_operation_abort_by_owners(self):
        spec = {"owners": [self.manage_and_read_user]}
        with self._run_op_context_manager(spec=spec) as (op, job_id):
            self._validate_access(self.no_rights_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.read_only_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.manage_only_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.banned_from_managing_user, False, _abort_op, operation_id=op.id)
            self._validate_access(self.manage_and_read_user, True, _abort_op, operation_id=op.id)

    @authors("omgronny")
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

    @authors("omgronny")
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

    @authors("omgronny")
    def test_acl_errors(self):
        # Wrong permissions.
        with pytest.raises(YtError):
            with self._run_op_context_manager(
                spec={
                    "acl": [make_ace("allow", self.manage_and_read_user, ["read", "write"])],
                }
            ):
                pass

    @authors("omgronny")
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

    @authors("omgronny")
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

    @authors("omgronny", "aleksandr.gaev")
    @pytest.mark.parametrize("use_acl", [False, True])
    @pytest.mark.parametrize("should_archive_operation", [False, True])
    @pytest.mark.parametrize("include_runtime", [False, True])
    def test_aco_get_operation(self, use_acl, should_archive_operation, include_runtime):
        spec = self.spec if use_acl else {"aco_name": "users"}
        op = run_test_vanilla(
            command=with_breakpoint("BREAKPOINT"),
            spec=spec,
        )
        wait_breakpoint()

        if should_archive_operation:
            release_breakpoint()
            clean_operations()

        self._validate_access(self.no_rights_user, True, get_operation, op_id_or_alias=op.id, include_runtime=include_runtime)
        self._validate_access(self.read_only_user, True, get_operation, op_id_or_alias=op.id, include_runtime=include_runtime)
        self._validate_access(self.manage_only_user, True, get_operation, op_id_or_alias=op.id, include_runtime=include_runtime)
        self._validate_access(self.manage_and_read_user, True, get_operation, op_id_or_alias=op.id, include_runtime=include_runtime)
        self._validate_access(self.banned_from_managing_user, True, get_operation, op_id_or_alias=op.id, include_runtime=include_runtime)
        self._validate_access(self.banned_user, True, get_operation, op_id_or_alias=op.id, include_runtime=include_runtime)

    @authors("omgronny")
    def test_aco_in_operations(self):
        op = run_test_vanilla(
            command=with_breakpoint("BREAKPOINT"),
            spec={
                "aco_name": "users",
            }
        )

        (job_id,) = wait_breakpoint()

        actions = [
            get_job_fail_context,
            get_job_stderr,
        ]

        for action in actions:
            self._validate_access(self.no_rights_user, False, action, operation_id=op.id, job_id=job_id)
            self._validate_access(self.read_only_user, True, action, operation_id=op.id, job_id=job_id)
            self._validate_access(self.manage_only_user, False, action, operation_id=op.id, job_id=job_id)
            self._validate_access(self.manage_and_read_user, True, action, operation_id=op.id, job_id=job_id)
            self._validate_access(self.banned_from_managing_user, True, action, operation_id=op.id, job_id=job_id)

    @authors("omgronny")
    def test_aco_and_acl_in_operations(self):
        acl = self.spec["acl"]

        with raises_yt_error(yt_error_codes.Scheduler.CannotUseBothAclAndAco):
            run_test_vanilla(
                command="sleep 1",
                spec={
                    "aco_name": "users",
                    "acl": acl,
                }
            )

        op = run_test_vanilla(
            command=with_breakpoint("BREAKPOINT"),
            spec={
                "aco_name": "users",
            }
        )

        wait_breakpoint()

        with raises_yt_error(yt_error_codes.Scheduler.CannotUseBothAclAndAco):
            update_op_parameters(op.id, parameters={"acl": acl})

        update_op_parameters(op.id, parameters={"aco_name": "new_aco"})
        wait(lambda: get_operation(op.id)["runtime_parameters"]["aco_name"] == "new_aco")

    @authors("omgronny")
    def test_manage_operation_actions_using_aco(self):
        spec = {
            "aco_name": "users",
        }

        actions = [
            _abort_op,
            _complete_op,
            suspend_and_resume_op,
            _update_op_parameters,
        ]

        for action in actions:
            with self._run_op_context_manager(spec=spec) as (
                op,
                _,
            ):
                self._validate_access(self.no_rights_user, False, action, operation_id=op.id)
                self._validate_access(self.read_only_user, False, action, operation_id=op.id)
                self._validate_access(self.banned_from_managing_user, False, action, operation_id=op.id)
                self._validate_access(self.manage_only_user, True, action, operation_id=op.id)
            with self._run_op_context_manager(spec=spec) as (
                op,
                _,
            ):
                self._validate_access(self.manage_and_read_user, True, action, operation_id=op.id)

    @authors("omgronny")
    def test_missing_user_in_acl(self):
        create_test_tables(force=True)
        op = map(
            command=with_breakpoint("BREAKPOINT; cat"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            track=False,
            spec={
                "acl": [make_ace("allow", "missing_user", ["read", "manage"])],
            }
        )
        wait(lambda: op.get_state() == "running")
        assert not op.get_alerts()
        update_op_parameters(
            op.id,
            parameters={"acl": [make_ace("allow", "another_missing_user", ["read", "manage"])]},
        )
        time.sleep(0.1)
        assert not op.get_alerts()

    @authors("aleksandr.gaev")
    @pytest.mark.parametrize("use_acl", [False, True])
    @pytest.mark.parametrize("should_archive_operation", [False, True])
    def test_get_job(self, use_acl, should_archive_operation):
        spec = self.spec if use_acl else {"aco_name": "users"}
        spec["max_failed_job_count"] = 1
        op = run_test_vanilla(
            command=with_breakpoint("BREAKPOINT; exit 1"),
            spec=spec,
        )

        (job_id,) = wait_breakpoint()

        if should_archive_operation:
            release_breakpoint()
            clean_operations()

        # Wait for job to become available.
        retry(lambda: get_job(op.id, job_id))

        self._validate_access(self.no_rights_user, True, get_job, operation_id=op.id, job_id=job_id)
        self._validate_access(self.read_only_user, True, get_job, operation_id=op.id, job_id=job_id)
        self._validate_access(self.manage_only_user, True, get_job, operation_id=op.id, job_id=job_id)
        self._validate_access(self.manage_and_read_user, True, get_job, operation_id=op.id, job_id=job_id)
        self._validate_access(self.banned_from_managing_user, True, get_job, operation_id=op.id, job_id=job_id)
        self._validate_access(self.banned_user, True, get_job, operation_id=op.id, job_id=job_id)

    @authors("aleksandr.gaev")
    @pytest.mark.parametrize("use_acl", [False, True])
    @pytest.mark.parametrize("should_archive_operation", [False, True])
    def test_list_jobs(self, use_acl, should_archive_operation):
        spec = self.spec if use_acl else {"aco_name": "users"}
        spec["max_failed_job_count"] = 1
        op = run_test_vanilla(
            command=with_breakpoint("BREAKPOINT; exit 1"),
            spec=spec,
        )

        wait_breakpoint()

        if should_archive_operation:
            release_breakpoint()
            clean_operations()

        self._validate_access(self.no_rights_user, True, list_jobs, operation_id=op.id)
        self._validate_access(self.read_only_user, True, list_jobs, operation_id=op.id)
        self._validate_access(self.manage_only_user, True, list_jobs, operation_id=op.id)
        self._validate_access(self.manage_and_read_user, True, list_jobs, operation_id=op.id)
        self._validate_access(self.banned_from_managing_user, True, list_jobs, operation_id=op.id)
        self._validate_access(self.banned_user, True, list_jobs, operation_id=op.id)

    @authors("aleksandr.gaev")
    @pytest.mark.parametrize("use_acl", [False, True])
    @pytest.mark.parametrize("should_archive_operation", [False, True])
    def test_list_operations(self, use_acl, should_archive_operation):
        spec = self.spec if use_acl else {"aco_name": "users"}
        before_start_time = datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING)
        op = run_test_vanilla(
            command=with_breakpoint("BREAKPOINT"),
            spec=spec,
        )
        after_start_time = datetime.utcnow().strftime(YT_DATETIME_FORMAT_STRING)
        wait_breakpoint()

        if should_archive_operation:
            release_breakpoint()
            clean_operations()
            cypress_operations = []
            all_operations = [op.id]
        else:
            cypress_operations = [op.id]
            all_operations = [op.id]

        def validate_operations(list_result, expected):
            assert [op["id"] for op in list_result["operations"]] == expected

        validate_operations(list_operations(authenticated_user=self.no_rights_user), cypress_operations)
        validate_operations(list_operations(authenticated_user=self.read_only_user), cypress_operations)
        validate_operations(list_operations(authenticated_user=self.manage_only_user), cypress_operations)
        validate_operations(list_operations(authenticated_user=self.manage_and_read_user), cypress_operations)
        validate_operations(list_operations(authenticated_user=self.banned_from_managing_user), cypress_operations)
        validate_operations(list_operations(authenticated_user=self.banned_user), cypress_operations)

        validate_operations(
            list_operations(
                authenticated_user=self.no_rights_user,
                include_archive=True,
                from_time=before_start_time,
                to_time=after_start_time,
            ),
            all_operations,
        )
        validate_operations(
            list_operations(
                authenticated_user=self.read_only_user,
                include_archive=True,
                from_time=before_start_time,
                to_time=after_start_time,
            ),
            all_operations,
        )
        validate_operations(
            list_operations(
                authenticated_user=self.manage_only_user,
                include_archive=True,
                from_time=before_start_time,
                to_time=after_start_time,
            ),
            all_operations,
        )
        validate_operations(
            list_operations(
                authenticated_user=self.manage_and_read_user,
                include_archive=True,
                from_time=before_start_time,
                to_time=after_start_time,
            ),
            all_operations,
        )
        validate_operations(
            list_operations(
                authenticated_user=self.banned_from_managing_user,
                include_archive=True,
                from_time=before_start_time,
                to_time=after_start_time,
            ),
            all_operations,
        )
        validate_operations(
            list_operations(
                authenticated_user=self.banned_user,
                include_archive=True,
                from_time=before_start_time,
                to_time=after_start_time,
            ),
            all_operations,
        )
