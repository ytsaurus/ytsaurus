import logging
import os
import time
from enum import Enum
from collections import namedtuple
import signal
import pytest
import spyt.client as client
import spyt.submit as submit
import spyt.utils as utils
from yt.wrapper import YtClient
from yt.wrapper.cypress_commands import list as yt_list, remove, exists


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)

e2e_home_path = os.environ['E2E_TEST_HOME_PATH']
user_dir_path = os.environ['E2E_TEST_UDIR_PATH']

scripts_path = user_dir_path + "/scripts"
python_dir_path = user_dir_path + "/python"

proxy = os.environ['PROXIES']
discovery_path = os.environ['DISCOVERY_PATH']
client_version = os.environ['CLIENT_VERSION']


@pytest.fixture(scope="module")
def submission_client():
    with submit.java_gateway() as gateway:
        submission_client = submit.SparkSubmissionClient(gateway, proxy, discovery_path, client_version,
                                                         utils.default_user(), utils.default_token())
        yield submission_client


@pytest.fixture(scope="module")
def yt_client():
    yield client.create_yt_client(proxy, {})


class TimeoutExecution:
    def __init__(self, sec):
        self.sec = sec

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)

    def __exit__(self, *args):
        signal.alarm(0)

    def raise_timeout(self, *args):
        raise RuntimeError('Timeout exceed')


class CheckResult(Enum):
    OK = "ok"
    EMPTY = "empty"
    WRONG_DATA = "wrong_answer"
    WRONG_SCHEMA = "wrong_schema"
    UNEXPECTED_ERROR = "unexpected_error"
    UNKNOWN = "unknown"


E2ETestCaseBase = namedtuple(
    'E2ETestCaseBase', [
        'name',
        'key_columns',
        'execution_time',
        'custom_input_path',
        'unique_keys',
        'conf'
    ])


class E2ETestCase(E2ETestCaseBase):
    def job_path(self):
        return "yt:/" + scripts_path + "/" + self.name + ".py"

    def case_python_dir_path(self):
        return python_dir_path + "/" + self.name

    def output_path(self):
        return self.case_python_dir_path() + "/output"

    def check_result_path(self):
        return self.case_python_dir_path() + "/check_result"

    def case_e2e_home_path(self):
        return e2e_home_path + "/" + self.name

    def input_path(self):
        return self.custom_input_path or self.case_e2e_home_path() + "/input"

    def expected_path(self):
        return self.case_e2e_home_path() + "/expected"

    def with_conf(self, key, value):
        # type: (str, str) -> E2ETestCase
        new_conf = dict(self.conf)
        new_conf[key] = value
        return self._replace(conf=new_conf)


def make_e2e_test_case(name, key_columns, execution_time=None, custom_input_path=None, unique_keys=None, conf=None):
    execution_time = execution_time or 0
    unique_keys = unique_keys or False
    conf = conf or {
        "spark.pyspark.python": "/opt/python3.7/bin/python3.7",
        "spark.yt.read.keyColumnsFilterPushdown.enabled": "true"
    }
    return E2ETestCase(name, key_columns, execution_time, custom_input_path,
                       unique_keys,
                       conf)


def read_check_result_without_details(yt_client, path):
    # type: (YtClient, str) -> CheckResult
    files = yt_list(path, client=yt_client)
    if len(files) != 1:
        return CheckResult.UNKNOWN
    else:
        result = files[0]
        if result == "_OK":
            return CheckResult.OK
        elif result == "_WRONG_ANSWER":
            return CheckResult.WRONG_DATA
        elif result == "_EMPTY":
            return CheckResult.EMPTY
        elif result == "_WRONG_SCHEMA":
            return CheckResult.WRONG_SCHEMA
        elif result == "_UNEXPECTED_ERROR":
            return CheckResult.UNEXPECTED_ERROR
        else:
            return CheckResult.UNKNOWN


def wait_final_status(submission_client, app_id):
    # type: (submit.SparkSubmissionClient, str) -> submit.SubmissionStatus
    status = submission_client.get_status(app_id)
    while not submit.SubmissionStatus.is_final(status):
        status = submission_client.get_status(app_id)
        time.sleep(5)
    return status


def prepare_job_launcher(submission_client, test_case):
    # type: (submit.SparkSubmissionClient, E2ETestCase) -> submit.SparkLauncher
    launcher = submission_client.new_launcher()
    launcher.set_app_name(test_case.name)
    launcher.set_app_resource(test_case.job_path())
    launcher.add_app_args(
        test_case.input_path(),
        test_case.output_path()
    )
    for key, value in test_case.conf.items():
        launcher.set_conf(key, value)
    return launcher


def submit_job(submission_client, launcher):
    # type: (submit.SparkSubmissionClient, submit.SparkLauncher) -> str
    return submission_client.submit(launcher, submit.RetryConfig(False, 0))


def run_job(submission_client, test_case):
    # type: (submit.SparkSubmissionClient, E2ETestCase) -> None
    launcher = prepare_job_launcher(submission_client, test_case)
    app_id = submit_job(submission_client, launcher)
    status = wait_final_status(submission_client, app_id)
    assert submit.SubmissionStatus.is_success(status), "Job " + test_case.name + " failed, " + str(status)


def run_check(submission_client, yt_client, test_case):
    # type: (submit.SparkSubmissionClient, YtClient, E2ETestCase) -> CheckResult
    launcher = submission_client.new_launcher()
    launcher.set_app_name(test_case.name + "_check")
    launcher.set_app_resource("yt:/" + user_dir_path + "/check.jar")
    launcher.set_main_class("tech.ytsaurus.spark.e2e.check.CheckApp")
    launcher.add_app_args(
        "--actual",
        test_case.output_path(),
        "--expected",
        test_case.expected_path(),
        "--result",
        test_case.check_result_path()[1:],
        "--keys",
        ",".join(test_case.key_columns),
        "--uniqueKeys",
        str(test_case.unique_keys)
    )
    launcher.set_conf("spark.sql.schema.forcingNullableIfNoMetadata.enabled", "true")

    app_id = submit_job(submission_client, launcher)
    status = wait_final_status(submission_client, app_id)
    assert submit.SubmissionStatus.is_success(status), "Job " + test_case.name + "_check failed, " + str(status)

    return read_check_result_without_details(yt_client, test_case.check_result_path())


def run_test(submission_client, yt_client, test_case):
    # type: (submit.SparkSubmissionClient, YtClient, E2ETestCase) -> None
    logging.info("Start job " + test_case.name)
    run_job(submission_client, test_case)
    logging.info("Finished job " + test_case.name)

    logging.info("Check job " + test_case.name)
    res = run_check(submission_client, yt_client, test_case)
    logging.info("Finished check " + test_case.name)
    assert res == CheckResult.OK


def test_link_eda_user_appsession_request_id(submission_client, yt_client):
    test_case = make_e2e_test_case("link_eda_user_appsession_request_id", ["appsession_id"])
    with TimeoutExecution(160):
        run_test(submission_client, yt_client, test_case)


def test_fct_extreme_user_order_act(submission_client, yt_client):
    test_case = make_e2e_test_case("fct_extreme_user_order_act", ["phone_pd_id"]) \
        .with_conf("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    with TimeoutExecution(200):
        run_test(submission_client, yt_client, test_case)


def test_getting_active_drivers(submission_client):
    test_case = make_e2e_test_case("link_eda_user_appsession_request_id_no_output", ["appsession_id"],
                                   custom_input_path=e2e_home_path + "/link_eda_user_appsession_request_id/input")
    with TimeoutExecution(200):
        assert submission_client.get_active_drivers() == []

        job1_id = submit_job(submission_client, prepare_job_launcher(submission_client, test_case))
        assert submission_client.get_active_drivers() == [job1_id]

        job2_id = submit_job(submission_client, prepare_job_launcher(submission_client, test_case))
        assert sorted(submission_client.get_active_drivers()) == sorted([job1_id, job2_id])

        while len(submission_client.get_active_drivers()) > 0:
            time.sleep(5)

        status1 = submission_client.get_status(job1_id)
        status2 = submission_client.get_status(job2_id)
        assert submit.SubmissionStatus.is_success(status1), "Job " + test_case.name + " failed, " + str(status1)
        assert submit.SubmissionStatus.is_success(status2), "Job " + test_case.name + " failed, " + str(status2)


def test_killing_driver(submission_client):
    test_case = make_e2e_test_case("link_eda_user_appsession_request_id_no_output", ["appsession_id"],
                                   custom_input_path=e2e_home_path + "/link_eda_user_appsession_request_id/input")
    with TimeoutExecution(40):
        job_id = submit_job(submission_client, prepare_job_launcher(submission_client, test_case))

        assert submission_client.kill(job_id), "Job is not killed"
        time.sleep(2)

        status = submission_client.get_status(job_id)
        assert status == submit.SubmissionStatus.KILLED, "Job " + test_case.name + " failed, " + str(status)


def test_processing_failed_job(submission_client):
    test_case = make_e2e_test_case("link_eda_user_appsession_request_id_fail", ["appsession_id"],
                                   custom_input_path=e2e_home_path + "/link_eda_user_appsession_request_id/input")
    with TimeoutExecution(40):
        job_id = submit_job(submission_client, prepare_job_launcher(submission_client, test_case))
        status = wait_final_status(submission_client, job_id)
        assert status == submit.SubmissionStatus.FAILED, "Job " + test_case.name + " not failed, " + str(status)

