import logging
import os
import time
from enum import Enum
from typing import NamedTuple, List, Dict, Optional

import pytest
import spyt.client as client
import spyt.submit as submit
import spyt.utils as utils
from yt.wrapper import YtClient
from yt.wrapper.cypress_commands import list as yt_list, remove, exists


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)

e2e_home_path = os.environ['e2eTestHomePath']
user_dir_path = os.environ['e2eTestUDirPath']

scripts_path = f"{user_dir_path}/scripts"
python_dir_path = f"{user_dir_path}/python"

proxy = os.environ['proxies']
discovery_path = f"{e2e_home_path}/cluster"
client_version = os.environ['clientVersion']


@pytest.fixture(scope="module")
def submission_client():
    with submit.java_gateway() as gateway:
        submission_client = submit.SparkSubmissionClient(gateway, proxy, discovery_path, client_version,
                                                         utils.default_user(), utils.default_token())
        yield submission_client


@pytest.fixture(scope="module")
def yt_client():
    yield client.create_yt_client(proxy, {})


class CheckResult(Enum):
    OK = "ok"
    EMPTY = "empty"
    WRONG_DATA = "wrong_answer"
    WRONG_SCHEMA = "wrong_schema"
    UNEXPECTED_ERROR = "unexpected_error"
    UNKNOWN = "unknown"


class E2ETestCase(NamedTuple):
    name: str
    key_columns: List[str]
    execution_time: int = 0
    custom_input_path: Optional[str] = None
    unique_keys: bool = False
    conf: Dict[str, str] = {
        "spark.pyspark.python": "/opt/python3.7/bin/python3.7",
        "spark.yt.read.keyColumnsFilterPushdown.enabled": "true"
    }

    def job_path(self):
        return f"yt:/{scripts_path}/{self.name}.py"

    def case_python_dir_path(self):
        return f"{python_dir_path}/{self.name}"

    def output_path(self):
        return f"{self.case_python_dir_path()}/output"

    def check_result_path(self):
        return f"{self.case_python_dir_path()}/check_result"

    def case_e2e_home_path(self):
        return f"{e2e_home_path}/{self.name}"

    def input_path(self):
        return self.custom_input_path or f"{self.case_e2e_home_path()}/input"

    def expected_path(self):
        return f"{self.case_e2e_home_path()}/expected"

    def with_conf(self, key: str, value: str):
        new_conf = dict(self.conf)
        new_conf[key] = value
        return self._replace(conf=new_conf)


def read_check_result_without_details(yt_client: YtClient, path: str) -> CheckResult:
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


def submit_and_join(submission_client: submit.SparkSubmissionClient,
                    launcher: submit.SparkLauncher) -> submit.SubmissionStatus:
    app_id = submission_client.submit(launcher)
    status = submission_client.get_status(app_id)
    while not submit.SubmissionStatus.is_final(status):
        status = submission_client.get_status(app_id)
        time.sleep(5)
    return status


def run_job(submission_client: submit.SparkSubmissionClient, test_case: E2ETestCase):
    launcher = submission_client.new_launcher()
    launcher.set_app_name(test_case.name)
    launcher.set_app_resource(test_case.job_path())
    launcher.add_app_args(
        test_case.input_path(),
        test_case.output_path()
    )
    for key, value in test_case.conf.items():
        launcher.set_conf(key, value)

    status = submit_and_join(submission_client, launcher)
    assert submit.SubmissionStatus.is_success(status), f"Job {test_case.name} failed, {status}"


def run_check(submission_client: submit.SparkSubmissionClient, yt_client: YtClient,
              test_case: E2ETestCase) -> CheckResult:
    launcher = submission_client.new_launcher()
    launcher.set_app_name(f"{test_case.name}_check")
    launcher.set_app_resource(f"yt:/{user_dir_path}/check.jar")
    launcher.set_main_class("ru.yandex.spark.e2e.check.CheckApp")
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

    status = submit_and_join(submission_client, launcher)
    assert submit.SubmissionStatus.is_success(status), f"Job {test_case.name}_check failed, {status}"

    return read_check_result_without_details(yt_client, test_case.check_result_path())


def run_test(submission_client: submit.SparkSubmissionClient, yt_client: YtClient, test_case: E2ETestCase):
    logging.info(f"Start job {test_case.name}")
    run_job(submission_client, test_case)
    logging.info(f"Finished job {test_case.name}")

    logging.info(f"Check job ${test_case.name}")
    res = run_check(submission_client, yt_client, test_case)
    logging.info(f"Finished check {test_case.name}")
    assert res == CheckResult.OK


def test_link_eda_user_appsession_request_id(submission_client, yt_client):
    test_case = E2ETestCase("link_eda_user_appsession_request_id", ["appsession_id"])
    run_test(submission_client, yt_client, test_case)


def test_link_eda_user_appsession_request_id_python2(submission_client, yt_client):
    test_case = E2ETestCase("link_eda_user_appsession_request_id_python2", ["appsession_id"],
                            custom_input_path=f"{e2e_home_path}/link_eda_user_appsession_request_id/input") \
        .with_conf("spark.pyspark.python", "python2.7")
    run_test(submission_client, yt_client, test_case)


def test_fct_extreme_user_order_act(submission_client, yt_client):
    test_case = E2ETestCase("fct_extreme_user_order_act", ["phone_pd_id"]) \
        .with_conf("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    run_test(submission_client, yt_client, test_case)
