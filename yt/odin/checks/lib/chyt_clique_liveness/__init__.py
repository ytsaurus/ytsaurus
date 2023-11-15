import yt.packages.requests as requests
from yt.packages.requests.adapters import HTTPAdapter
from yt.wrapper.transaction_commands import make_request
from yt.common import YtError
from yt.wrapper.http_helpers import get_token, get_proxy_address_url

import datetime
import json
import random
import sys
import time
import signal

SORTED_NUMBERS = list(range(0, 1000))
UNSORTED_NUMBERS = list(SORTED_NUMBERS)
random.seed(888)
random.shuffle(UNSORTED_NUMBERS)
TEST_DATA = [{'x': x} for x in UNSORTED_NUMBERS]  # list of dicts [{'x': 1}, {'x': 2}, ...]

TEMP_PATH = "//tmp/{}_clique_check"
QUERY = 'SELECT x FROM "{}" ORDER BY x FORMAT JSON'
TIMEOUT = 30
FAILED_JOBS_CHECK_MINUTES = 3
SIGNALS = dict((k, v) for v, k in reversed(sorted(signal.__dict__.items()))
               if v.startswith('SIG') and not v.startswith('SIG_'))


def find_operation(alias, yt_client, logger):
    op_id = None
    state = None
    try:
        operation_str = make_request("get_operation", {"operation_alias": alias, "include_runtime": True,
                                                       "attributes": ['id', 'state']}, client=yt_client)
        operation = json.loads(operation_str)
        op_id = operation.get('id', None)
        state = operation.get('state', None)
    except:  # noqa
        logger.exception('Failed to find operation with alias: %s', alias)

    if op_id and state == 'running':
        cluster = yt_client.config.get('proxy', {}).get('url', '').split('.')[0]
        logger.info('Alias %s belongs to running operation %s', alias, op_id)
        if cluster:
            logger.info('https://yt.yandex-team.ru/%s/operations/%s', cluster, op_id)
        return op_id
    return None


def get_failed_and_aborted_jobs(op_id, yt_client, interval):
    result = []
    failed_jobs = yt_client.list_jobs(op_id, job_state='failed', limit=10,
                                      sort_field='finish_time', sort_order='descending', data_source='archive')
    aborted_jobs = yt_client.list_jobs(op_id, job_state='aborted', limit=10,
                                       sort_field='finish_time', sort_order='descending', data_source='archive')
    jobs = []
    jobs.extend(failed_jobs.get('jobs', []))
    jobs.extend(aborted_jobs.get('jobs', []))

    jobs = filter(lambda job: job.get('finish_time') is not None, jobs)
    jobs = sorted(jobs, key=lambda job: job.get('finish_time'), reverse=True)

    for job in jobs:
        finish_time = datetime.datetime.strptime(job.get('finish_time'), "%Y-%m-%dT%H:%M:%S.%fZ")
        if finish_time >= datetime.datetime.utcnow() - interval:
            result.append(job)

    return result


def represent_error(error):
    if error is None:
        return "(n/a)"

    tags = []

    # TODO(max42): YT-12048.
    if error._contains_text("produced core files"):
        tags.append("core dumped")

    error_text = None

    if error_text is None:
        non_zero_exit_code_error = error.find_matching_error(code=10000)
        if non_zero_exit_code_error is not None:
            exit_code = non_zero_exit_code_error.attributes["exit_code"]
            error_text = "Process exited with code {}".format(exit_code)
            if exit_code == 42:
                tags.append("OOM by watchdog")
            elif exit_code >= 128:
                signal_value = 256 - exit_code if exit_code > 192 else exit_code - 128
                signal_name = SIGNALS.get(signal_value, signal_value)
                tags.append(str(signal_name))

    if error_text is None:
        memory_limit_exceeded_error = error.find_matching_error(code=1200)
        if memory_limit_exceeded_error is not None:
            error_text = "OOM"

    if error_text is None:
        user_job_failed_error = error.find_matching_error(code=1200)
        if user_job_failed_error is not None:
            error_text = str(user_job_failed_error.inner_errors[0])

    if error_text is None:
        error_text = error.message

    return error_text + "".join(" [{}]".format(tag) for tag in tags)


def has_failed_jobs(alias, client, interval, logger):
    logger.info("Going to check failed jobs...")
    op_id = find_operation(alias, client, logger)
    if not op_id:
        return False

    jobs = get_failed_and_aborted_jobs(op_id, client, interval)
    if len(jobs) == 0:
        logger.info("There were no failed nor aborted jobs for %s minutes.",
                    interval.days * 24 * 3600 + interval.seconds // 60)
        return False

    logger.info("Failed or aborted jobs for last %s minutes:",
                interval.days * 24 * 3600 + interval.seconds // 60)
    for job in jobs:
        error = YtError(**job["error"]) if "error" in job else None
        error_text = represent_error(error)
        info = "  Job {} at {} {}: {}".format(job["id"], job["address"], job["state"], error_text)
        logger.info(info)

    return True


def create_test_table(yt_client, path, data, logger):
    if not yt_client.exists(path):
        yt_client.mkdir(path, recursive=True)
        logger.info("Created %s", path)

    schema = [
        {"name": "x", "type": "int64"}
    ]
    table_path = yt_client.create_temp_table(path=path, attributes={"schema": schema, 'optimize_for': 'scan'})
    logger.info("Created table: %s", table_path)

    table_writer = {
        "enable_early_finish": True,
        "upload_replication_factor": 3,
        "min_upload_replication_factor": 2,
    }

    yt_client.write_table(table_path, data, table_writer=table_writer)
    logger.info("Written %s rows", len(data))
    return table_path


def perform_test_query(proxy_url, alias, token, query, expected_result, logger):
    try:
        logger.info("Going to execute query: %s", query)
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=3))
        url = "{proxy}/query?database={alias}&password={token}".format(proxy=proxy_url, alias=alias, token=token)
        resp = s.post(url, data=query, timeout=TIMEOUT)
        if resp.status_code == 200:
            response = json.loads(resp.content)
            result = [int(row.get('x', 0)) for row in response.get('data', [])]
            if result != expected_result:
                logger.info("Result for %s doesn't match: %s != %s", query, result, expected_result)
                logger.info("Returned result: %s", result)
                logger.info("Expected result: %s", expected_result)
                return False
            return True
        else:
            logger.info("Response status: %s", resp.status_code)
            logger.error("Response content: %s", resp.content)
            logger.error("Response headers: %s", resp.headers)
    except:  # noqa
        logger.exception("Failed to execute test query %s", query)
    return False


def run_check_impl(yt_client, logger, options, states, clique_alias):
    soft_select_timeout = options.get("soft_select_timeout", sys.maxsize)
    test_query = options.get("test_query", QUERY)
    expected_result = options.get("expected_result", SORTED_NUMBERS)
    temp_path = options.get("temp_tables_path", TEMP_PATH.format(clique_alias.replace('*', '')))
    test_data = options.get("test_data", TEST_DATA)
    failed_jobs_check_minutes = options.get("failed_jobs_check_minutes", FAILED_JOBS_CHECK_MINUTES)

    if not yt_client.exists("//sys/clickhouse"):
        logger.error("Clickhouse not installed on this cluster")
        return states.UNAVAILABLE_STATE

    token = get_token(client=yt_client)
    if token is None:
        logger.error("Failed to get YT_TOKEN")
        return states.UNAVAILABLE_STATE

    check_result = states.UNAVAILABLE_STATE
    table_path = create_test_table(yt_client, temp_path, test_data, logger)
    try:
        start_time = int(time.time())
        if not perform_test_query(get_proxy_address_url(client=yt_client), clique_alias, token,
                                  test_query.format(table_path), expected_result, logger):
            return check_result
        else:
            now = int(time.time())
            if now - start_time > soft_select_timeout:
                logger.warning("Test query executed successfully but took "
                               "more than %d seconds", soft_select_timeout)
                check_result = states.PARTIALLY_AVAILABLE_STATE
            else:
                logger.info("Test query executed successfully")
                interval = datetime.timedelta(minutes=failed_jobs_check_minutes)
                if has_failed_jobs(clique_alias, yt_client, interval, logger):
                    check_result = states.PARTIALLY_AVAILABLE_STATE
                else:
                    check_result = states.FULLY_AVAILABLE_STATE
    finally:
        yt_client.remove(table_path, force=True)

    return check_result
