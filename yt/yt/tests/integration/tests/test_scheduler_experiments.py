import __builtin__

from yt_env_setup import YTEnvSetup
from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree,
    create_data_center, create_rack,
    make_ace, check_permission, add_member,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, lock,
    read_file, write_file, read_table, write_table,
    map, reduce, map_reduce, join_reduce, merge, vanilla, sort, erase,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, list_jobs, get_job, abandon_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec,
    dump_job_context, poll_job_shell,
    abort_op, complete_op, suspend_op, resume_op,
    get_operation, list_operations, clean_operations,
    get_operation_cypress_path, scheduler_orchid_pool_path,
    scheduler_orchid_default_pool_tree_path, scheduler_orchid_operation_path,
    scheduler_orchid_default_pool_tree_config_path, scheduler_orchid_path,
    scheduler_orchid_node_path, scheduler_orchid_pool_tree_config_path,
    sync_create_cells, sync_mount_table,
    get_first_chunk_id, get_singular_chunk_id, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag,
    check_all_stderrs,
    create_test_tables, PrepareTables,
    get_statistics,
    make_random_string, raises_yt_error,
    normalize_schema, make_schema)

import yt.environment.init_operation_archive as init_operation_archive

from flaky import flaky
import math

##################################################################


class TestSchedulerExperiments(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 2

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "experiments": {
                "exp_a1": {
                    "fraction": 0.6,
                    "ticket": "YTEXP-1",
                    "ab_treatment_group": {
                        "fraction": 0.5,
                        "scheduler_spec_template_patch": {
                            "foo_spec_template": "exp_a1.treatment",
                        },
                        "scheduler_spec_patch": {
                            "foo_spec": "exp_a1.treatment",
                        },
                    },
                },
                "exp_a2": {
                    "fraction": 0.3,
                    "ticket": "YTEXP-2",
                    "groups": {
                        "group1": {
                            "fraction": 1.0 / 3,
                            "scheduler_spec_patch": {
                                "foo_spec": "exp_a2.group1",
                            },
                        },
                        "group2": {
                            "fraction": 2.0 / 3,
                            "scheduler_spec_patch": {
                                "foo_spec": "exp_a2.group1",
                            },
                        },
                    },
                },
                "exp_b1": {
                    "fraction": 0.5,
                    "ticket": "YTEXP-3",
                    "dimension": "other_dimension",
                    "ab_treatment_group": {
                        "fraction": 1.0,
                        "controller_agent_tag": "tagged",
                        "scheduler_spec_patch": {
                            "foo_spec": "exp_b1.treatment",
                        },
                    },
                },
                "exp_b2": {
                    "fraction": 0.3,
                    "ticket": "YTEXP-4",
                    "dimension": "other_dimension",
                    "filter": "[/type] = 'map'",
                    "ab_treatment_group": {
                        "fraction": 1.0,
                        "scheduler_spec_patch": {
                            "foo_spec": "exp_b2.treatment",
                        },
                    },
                },
            },
        },
    }

    controller_agent_tag_to_address = dict()
    controller_agent_counter = 0

    @classmethod
    def modify_controller_agent_config(cls, config):
        if cls.controller_agent_counter == 2:
            cls.controller_agent_counter = 0

        controller_agent_tags = ["tagged", "default"]

        tag = controller_agent_tags[cls.controller_agent_counter]
        config["controller_agent"]["tags"] = [tag]
        cls.controller_agent_tag_to_address[tag] = "localhost:" + str(config["rpc_port"])

        cls.controller_agent_counter += 1

    @classmethod
    def setup_class(cls, **kwargs):
        super(TestSchedulerExperiments, cls).setup_class(**kwargs)

    @authors("max42")
    def test_get_operation(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"a": 1}])

        op = map(in_=["//tmp/t_in"],
                 out=[],
                 command=with_breakpoint("BREAKPOINT"),
                 track=False,
                 spec={"experiment_overrides": ["exp_a1.treatment"]})
        wait_breakpoint()
        info = get_operation(op.id, attributes=["experiment_assignments", "experiment_assignment_names", "spec"])
        assert info["experiment_assignment_names"] == ["exp_a1.treatment"]
        assert info["experiment_assignments"] == [{
            "experiment": "exp_a1",
            "group": "treatment",
            "experiment_uniform_sample": 0.0,
            "group_uniform_sample": 0.0,
            "ticket": "YTEXP-1",
            "dimension": "default",
            "effect": {
                "fraction": 0.5,
                "scheduler_spec_template_patch": {
                    "foo_spec_template": "exp_a1.treatment",
                },
                "scheduler_spec_patch": {
                    "foo_spec": "exp_a1.treatment",
                },
            }
        }]
        assert info["spec"]["foo_spec_template"] == "exp_a1.treatment"
        assert info["spec"]["foo_spec"] == "exp_a1.treatment"

    @authors("max42")
    def test_scheduler_spec_patches(self):
        create("table", "//tmp/t_in")

        op = map(in_=["//tmp/t_in"],
                 out=[],
                 command="exit 0",
                 spec={"experiment_overrides": ["exp_a1"]})

        info = get_operation(op.id, attributes=["spec"])
        assert info["spec"]["foo_spec"] == "exp_a1.treatment"
        assert info["spec"]["foo_spec_template"] == "exp_a1.treatment"

        op = map(
            in_=["//tmp/t_in"],
            out=[],
            command="exit 0",
            spec={
                "experiment_overrides": ["exp_a1"],
                "foo_spec": "custom",
                "foo_spec_template": "custom",
            })

        info = get_operation(op.id, attributes=["spec"])
        assert info["spec"]["foo_spec"] == "exp_a1.treatment"
        assert info["spec"]["foo_spec_template"] == "custom"

    @authors("max42")
    def test_controller_agent_tag(self):
        def get_controller_agent_address(events):
            addresses = [event["attributes"].get("controller_agent_address") for event in events]
            addresses = [address for address in addresses if address is not None]
            addresses = list(__builtin__.set(addresses))
            assert len(addresses) == 1
            return addresses[0]

        create("table", "//tmp/t_in")

        op = map(in_=["//tmp/t_in"],
                 out=[],
                 command="exit 0",
                 spec={"experiment_overrides": ["exp_b1.control"]})

        assert get_controller_agent_address(get_operation(op.id, attributes=["events"])["events"]) == \
               TestSchedulerExperiments.controller_agent_tag_to_address["default"]

        op = map(in_=["//tmp/t_in"],
                 out=[],
                 command="exit 0",
                 spec={"experiment_overrides": ["exp_b1.treatment"]})

        assert get_controller_agent_address(get_operation(op.id, attributes=["events"])["events"]) == \
               TestSchedulerExperiments.controller_agent_tag_to_address["tagged"]

    @authors("max42")
    def test_wrong_experiment_override(self):
        create("table", "//tmp/t_in")

        with raises_yt_error('Experiment "nonexistent" is not known'):
            map(in_=["//tmp/t_in"],
                out=[],
                command="exit 0",
                spec={"experiment_overrides": ["nonexistent"]})

        with raises_yt_error('Group "nonexistent" is not known'):
            map(in_=["//tmp/t_in"],
                out=[],
                command="exit 0",
                spec={"experiment_overrides": ["exp_a1.nonexistent"]})

    @authors("max42")
    @flaky(max_runs=3)
    def test_distribution(self):
        create("table", "//tmp/t_in")
        # In order to distinguish current test run from other runs (note flaky decorator above)
        # we mark all operations from this test with a unique random guid taken from created table id.
        guid = get("//tmp/t_in/@id")
        operation_count = 200
        for i in xrange(operation_count):
            map(in_=["//tmp/t_in"],
                out=[],
                command="exit 0",
                spec={"annotations": {"tag": "distribution" + guid}},
                track=False)

        wait(lambda: get("//tmp/t_in/@lock_count") == 0)

        operations = list_operations(
            limit=1000,
            filter="distribution" + guid,
            attributes=["experiment_assignment_names"],
            include_archive=False,
        )["operations"]

        assert len(operations) == 200

        def extract_assignments(operation):
            assignment_names = operation["experiment_assignment_names"]
            assert len(assignment_names) <= 2
            dimension_to_name = {"default": None, "other_dimension": None}
            for assignment_name in assignment_names:
                if "exp_a" in assignment_name:
                    dimension = "default"
                elif "exp_b" in assignment_name:
                    dimension = "other_dimension"
                else:
                    assert False
                assert dimension_to_name[dimension] is None
                dimension_to_name[dimension] = assignment_name
            return dimension_to_name["default"], dimension_to_name["other_dimension"]

        assignments = [extract_assignments(operation) for operation in operations]

        def validate_count(default, other_dimension, expected_fraction):
            expected_count = expected_fraction * len(operations)
            # We use three sigma rule (cf. Wikipedia article) since we have a binomial distribution
            # which is pretty close to normal distribution under our number of experiments.
            # Single test is about to succeed with probability 99.7%, we conduct ~10 tests
            # and, finally, mark test as flaky setting up at most three runs. This should be robust
            # enough not to ever see this test broken in CI :)
            sigma = math.sqrt(len(operations) * expected_fraction * (1 - expected_fraction))
            lower_bound = math.floor(expected_count - 3 * sigma)
            upper_bound = math.floor(expected_count + 3 * sigma)
            print_debug("Validating count for ({}, {}) to be between {} and {} "
                        "(expected fraction = {:.3f}, expected value = {:.3f}, sigma = {:.3f})".format(
                            default, other_dimension, lower_bound, upper_bound,
                            expected_fraction, expected_count, sigma))

            def matches(assignment, expected):
                return expected == "*" or (assignment is not None and assignment.startswith(expected))

            actual_count = len([assignment for assignment in assignments if
                                matches(assignment[0], default) and matches(assignment[1], other_dimension)])
            print_debug("Actual count = {}".format(actual_count))

            assert lower_bound <= actual_count <= upper_bound

        validate_count("*", "*", 1.0)
        validate_count("*", "exp_b1", 0.5)
        validate_count("*", "exp_b1.treatment", 0.5)
        validate_count("*", "exp_b1.control", 0.0)
        validate_count("*", "exp_b2", 0.3)
        validate_count("*", "exp_b2.treatment", 0.3)
        validate_count("*", "exp_b2.control", 0.0)
        validate_count("exp_a1", "*", 0.6)
        validate_count("exp_a1.control", "*", 0.3)
        validate_count("exp_a1.treatment", "*", 0.3)
        validate_count("exp_a2", "*", 0.3)
        validate_count("exp_a2.group1", "*", 0.1)
        validate_count("exp_a2.group2", "*", 0.2)
        validate_count("exp_a1", "exp_b1", 0.3)
        validate_count("exp_a2.group1", "exp_b1", 0.05)

    @authors("max42")
    def test_filters(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        with raises_yt_error('Operation does not match filter of experiment "exp_b2"'):
            merge(in_=["//tmp/t_in"],
                  out="//tmp/t_out",
                  spec={"experiment_overrides": ["exp_b2.treatment"]})


class TestSchedulerExperimentsArchivation(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 2

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "experiments": {
                "exp": {
                    "fraction": 0.5,
                    "ticket": "YTEXP-1",
                    "ab_treatment_group": {
                        "fraction": 0.5,
                        "scheduler_spec_patch": {
                            "foo_spec": "exp_a1.treatment",
                        },
                    },
                },
            },
            "operations_cleaner": {
                "enable": True,
                # Analyze all operations each 100ms
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
                "remove_batch_timeout": 100,
                "archive_batch_timeout": 100,
                "max_removal_sleep_delay": 100,
            },
        },
    }

    @authors("max42")
    def test_archivation(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"a": 1}])

        op = map(in_=["//tmp/t_in"],
                 out=[],
                 command=with_breakpoint("BREAKPOINT"),
                 track=False,
                 spec={"experiment_overrides": ["exp"]})

        def operation_present_in_cypress():
            return exists(op.get_path())

        wait(lambda: operation_present_in_cypress())
        wait_breakpoint()

        cypress_info = get_operation(
            op.id,
            attributes=["experiment_assignments", "experiment_assignment_names", "spec"])
        release_breakpoint()

        wait(lambda: not operation_present_in_cypress())

        archive_info = get_operation(
            op.id,
            attributes=["experiment_assignments", "experiment_assignment_names", "spec"])

        assert cypress_info == archive_info
