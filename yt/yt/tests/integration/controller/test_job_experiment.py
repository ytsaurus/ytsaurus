from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, create_network_project, create_user,
    get, make_ace, map, print_debug, read_table, set, write_table)

from yt.common import update

import pytest

from collections import Counter


USER_NETWORK_PROJECT = "user_network_project"
USER_NETWORK_PROJECT_ID = 333


class TestJobExperiment(YTEnvSetup):
    NUM_TEST_PARTITIONS = 5

    NUM_SCHEDULERS = 1

    INPUT_TABLE = "//tmp/input_table"
    OUTPUT_TABLE = "//tmp/output_table"
    NETWORK_PROJECT = "experimental_network_project"
    NETWORK_PROJECT_ID = 777
    USER = "user"

    MAX_TRIES = 3

    @staticmethod
    def create_network_project(network_project, network_project_id):
        create_network_project(network_project)
        set(f"//sys/network_projects/{network_project}/@project_id", network_project_id)
        set(f"//sys/network_projects/{network_project}/@acl", [make_ace("allow", TestJobExperiment.USER, "use")])

    @staticmethod
    def setup(job_count):
        create("table", TestJobExperiment.INPUT_TABLE)
        create("table", TestJobExperiment.OUTPUT_TABLE)
        create_user(TestJobExperiment.USER)

        TestJobExperiment.create_network_project(TestJobExperiment.NETWORK_PROJECT, TestJobExperiment.NETWORK_PROJECT_ID)
        TestJobExperiment.create_network_project(USER_NETWORK_PROJECT, USER_NETWORK_PROJECT_ID)

        for key in range(job_count):
            write_table(f"<append=%true>{TestJobExperiment.INPUT_TABLE}", [{"k": key, "network_project": "NETWORK_PROJECT"}])

    @staticmethod
    def get_spec(user_slots, **options):
        spec = {
            "job_experiment": {
                "network_project": TestJobExperiment.NETWORK_PROJECT,
                "alert_on_any_treatment_failure": True,
            },
            "mapper": {
                "format": "json",
            },
            "data_weight_per_job": 1,
            "resource_limits": {
                "user_slots": user_slots,
            },
        }

        return update(spec, options)

    @staticmethod
    def run_map(command, job_count, user_slots, **options):
        op = map(
            in_=TestJobExperiment.INPUT_TABLE,
            out=TestJobExperiment.OUTPUT_TABLE,
            command=command,
            spec=TestJobExperiment.get_spec(user_slots, **options),
            authenticated_user=TestJobExperiment.USER,
        )

        assert get(f"{TestJobExperiment.INPUT_TABLE}/@row_count") == get(f"{TestJobExperiment.OUTPUT_TABLE}/@row_count")

        assert op.get_job_count("completed") == job_count

        return op

    @authors("galtsev")
    @pytest.mark.flaky(max_runs=5)
    @pytest.mark.timeout(300)
    def test_job_experiment_success(self):
        job_count = 20
        self.setup(job_count)

        command = (
            f"""
            if [ .$YT_NETWORK_PROJECT_ID == .{TestJobExperiment.NETWORK_PROJECT_ID} ]; then
                sed 's/NETWORK_PROJECT/treatment/';
            else
                sed 's/NETWORK_PROJECT/control/';
            fi
            """
        )

        for try_count in range(self.MAX_TRIES + 1):
            op = self.run_map(command, job_count, user_slots=2)

            assert op.get_job_count("failed") == 0

            counter = Counter([row["network_project"] for row in read_table(self.OUTPUT_TABLE)])
            print_debug(f"try = {try_count}, counter = {counter}")

            if counter["control"] >= 1 and counter["treatment"] >= 2:
                break

        assert try_count < self.MAX_TRIES

    @authors("galtsev")
    @pytest.mark.timeout(300)
    def test_job_experiment_failure(self):
        job_count = 7
        self.setup(job_count)

        command = (
            f"""
            if [ .$YT_NETWORK_PROJECT_ID == .{TestJobExperiment.NETWORK_PROJECT_ID} ]; then
                sed 's/NETWORK_PROJECT/treatment/g';
                exit 1;
            else
                sed 's/NETWORK_PROJECT/control/g';
            fi
            """
        )

        alert_count = 0

        for try_count in range(self.MAX_TRIES + 1):
            op = self.run_map(command, job_count, user_slots=2)

            assert op.get_job_count("failed") == 0

            counter = Counter([row["network_project"] for row in read_table(self.OUTPUT_TABLE)])
            print_debug(f"try = {try_count}, counter = {counter}")

            assert counter["control"] == job_count
            assert counter["treatment"] == 0

            assert op.get_job_count("aborted") == 0 or "mtn_experiment_failed" in op.get_alerts()

            if "mtn_experiment_failed" in op.get_alerts():
                alert_count += 1

            if op.get_job_count("aborted") >= 2:
                break

        assert try_count < self.MAX_TRIES

        assert alert_count >= 1

    @authors("galtsev")
    @pytest.mark.flaky(max_runs=5)
    @pytest.mark.parametrize("options", [
        {"fail_on_job_restart": True},
        {"max_speculative_job_count_per_task": 0},
        {"try_avoid_duplicating_jobs": True},
        {"mapper": {"network_project": USER_NETWORK_PROJECT}},
    ])
    @pytest.mark.timeout(300)
    def test_job_experiment_disabled(self, options):
        job_count = 7
        self.setup(job_count)

        command = (
            f"""
            if [ .$YT_NETWORK_PROJECT_ID == .{TestJobExperiment.NETWORK_PROJECT_ID} ]; then
                sed 's/NETWORK_PROJECT/treatment/g';
            else
                sed 's/NETWORK_PROJECT/control/g';
            fi
            """
        )

        op = self.run_map(command, job_count, user_slots=2, **options)

        assert op.get_job_count("failed") == 0

        counter = Counter([row["network_project"] for row in read_table(self.OUTPUT_TABLE)])
        print_debug(f"counter = {counter}")

        assert counter["control"] == job_count
        assert counter["treatment"] == 0

        assert op.get_job_count("aborted") == 0

    @authors("galtsev")
    @pytest.mark.timeout(300)
    def test_job_experiment_races(self):
        job_count = 10
        self.setup(job_count)

        command = (
            f"""
            if [ .$YT_NETWORK_PROJECT_ID == .{TestJobExperiment.NETWORK_PROJECT_ID} ]; then
                sed 's/NETWORK_PROJECT/treatment/';
            else
                sed 's/NETWORK_PROJECT/control/';
            fi
            """
        )

        for iterations in range(3):
            for try_count in range(self.MAX_TRIES + 1):
                op = self.run_map(command, job_count, user_slots=2 + iterations)

                assert op.get_job_count("failed") == 0

                if op.get_job_count("aborted") >= 1:
                    break

            assert try_count < self.MAX_TRIES

    @authors("galtsev")
    @pytest.mark.flaky(max_runs=5)
    @pytest.mark.timeout(600)
    def test_job_experiment_alert(self):
        job_count = 10
        self.setup(job_count)

        for control_failure_rate in range(2, 5):
            for treatment_failure_rate in range(2, 5):

                command = (
                    f"""
                    if [ .$YT_NETWORK_PROJECT_ID == .{TestJobExperiment.NETWORK_PROJECT_ID} ]; then
                        sed 's/NETWORK_PROJECT/treatment/g';
                        if [ $(($RANDOM % {treatment_failure_rate})) -eq 0 ]; then
                            exit 1;
                        fi;
                    else
                        sed 's/NETWORK_PROJECT/control/g';
                        if [ $(($RANDOM % {control_failure_rate})) -eq 0 ]; then
                            exit 1;
                        fi;
                    fi
                    """
                )

                op = self.run_map(command, job_count, user_slots=5, max_failed_job_count=1000)

                counter = Counter([row["network_project"] for row in read_table(self.OUTPUT_TABLE)])
                print_debug(f"control_failure_rate = {control_failure_rate}, treatment_failure_rate = {treatment_failure_rate}, counter = {counter}")

                if "mtn_experiment_failed" in op.get_alerts():
                    attributes = op.get_alerts()["mtn_experiment_failed"]["attributes"]
                    assert attributes["failed_control_job_count"] == op.get_job_count("failed")
                    assert attributes["failed_treatment_job_count"] <= op.get_job_count("aborted")
                    assert attributes["succeeded_treatment_job_count"] > 0 or counter["treatment"] == 0

                assert op.get_job_count("failed") + op.get_job_count("aborted") > 0
