#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import collections
import datetime
import json
import logging
import os
import re
import sys
import time

from logrep import LogrepError

ChytGrepResult = collections.namedtuple("ChytGrepResult", ["timestamp", "log_level", "log_category", "log_message", "node"])

class YqlTables(object):
    def __init__(self, tables_30min, tables_1d, complete):
        self.tables_30min = tables_30min
        self.tables_1d = tables_1d
        self.complete = complete
        self.all_tables = tables_30min + tables_1d


def extract_node_from_source_uri(source_uri):
    # Example:
    #   prt://yt@m31-sas.hahn.yt.yandex.net/yt/disk4/hahn-data/push-client/queue/master-m31-sas.debug.log
    m = re.search("@(.*[.]yandex[.]net)/", source_uri)
    if m == None:
        return "<node-is-unknown:{}>".format(source_uri)
    return m.group(1)

def _filter_table_paths(path_list, log_type, start_time, end_time):
    if log_type == "30min":
        delta = datetime.timedelta(minutes=60)
        start_time = datetime.datetime(
            year=start_time.year,
            month=start_time.month,
            day=start_time.day,
            hour=start_time.hour,
            minute=30 if start_time.minute >= 30 else 0)
        time_parse_func = lambda s: datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    elif log_type == "1d":
        delta = datetime.timedelta(days=1)
        start_time = datetime.datetime(
            year=start_time.year,
            month=start_time.month,
            day=start_time.day)
        time_parse_func = lambda s: datetime.datetime.strptime(s, "%Y-%m-%d")
    else:
        raise AssertionError("Bad log_type: {}".format(log_type))

    expected_times = set()
    t = start_time
    while t <= end_time:
        expected_times.add(t)
        t += delta

    tables = []
    for path in path_list:
        name = path.split("/")[-1]
        try:
            time = time_parse_func(name)
        except ValueError:
            logging.warn("cannot parse time from {}".format(path))

        if start_time <= time <= end_time:
            if time in expected_times:
                expected_times.remove(time)
            tables.append(path)

    full_result = len(expected_times) == 0
    return tables, full_result


def find_tables(ytclient, prefix, start_time, end_time):
    dir_30min = prefix + "/30min"
    dir_1d = prefix + "/1d"

    def full_list(dir_name):
        if not ytclient.exists(dir_name):
            return []
        return [(dir_name + "/" + name) for name in ytclient.list(dir_name)]

    res_30min, full = _filter_table_paths(full_list(dir_30min), "30min", start_time, end_time)
    if full:
        return YqlTables(
            tables_30min=res_30min,
            tables_1d=[],
            complete=True
        )
    res_1d, full = _filter_table_paths(full_list(dir_1d), "1d", start_time, end_time)
    if full:
        return YqlTables(
            tables_30min=[],
            tables_1d=res_1d,
            complete=True
        )

    return YqlTables(
        tables_30min=res_30min,
        tables_1d=res_1d,
        complete=False,
    )

CHYT_REQUEST_TEMPLATE = """
USE chyt.hahn;

SELECT
    "timestamp",
    "log_message",
    "log_level",
    "log_category",
    "cluster",
    "source_uri"
FROM {tables}
WHERE cluster='{cluster}' AND log_message LIKE '%{pattern}%'
ORDER BY timestamp;
"""

def get_chyt_query(table_list, cluster, pattern):
    table_list_str = ", ".join('"{}"'.format(t) for t in table_list)
    return CHYT_REQUEST_TEMPLATE.format(
        tables=table_list_str,
        cluster=cluster,
        pattern=pattern,
    )

YQL_REQUEST_TEMPLATE = """
USE hahn;

SELECT
    `timestamp`,
    `log_message`,
    `log_level`,
    `log_category`,
    `cluster`,
    `source_uri`
FROM CONCAT ({tables})
WHERE cluster="{cluster}" AND log_message LIKE '%{pattern}%'
ORDER BY `timestamp`;
"""

def get_yql_query(table_list, cluster, pattern):
    table_list_str = ", ".join("`{}`".format(t) for t in table_list)
    return YQL_REQUEST_TEMPLATE.format(
        tables=table_list_str,
        cluster=cluster,
        pattern=pattern,
    )

def get_yql_token():
    yql_token_file = os.path.expanduser("~/.yql/token")
    if not os.path.exists(yql_token_file):
        raise LogrepError(
            "Cannot find yql token, please get your token at:\n"
            "  https://yql.yandex-team.ru/?settings_mode=token\n"
            "and put it into ~/.yql/token\n")

    with open(yql_token_file) as inf:
        return inf.read().strip()


# NOTE: official YQL python client doesn't support CLICKHOUSE requests so we send HTTP requests by ourselves
#
# https://yql.yandex-team.ru/docs/yt/interfaces/http/
class YqlOperation(object):
    def __init__(self, token, query, flavour="CLICKHOUSE"):
        if flavour not in ["CLICKHOUSE", "SQLv1"]:
            raise ValueError("Bad type: {}".format(flavour))
        self.token = token
        self.query = query
        self.type = flavour
        self.id = None
        self.status = None

        try:
            import requests
        except ImportError:
            raise LogrepError(
                "Cannot import `requests` module.\n"
                "Please install it using:\n"
                "  $ pip2 install --user requests\n"
            )
        self.requests = requests

    def _get_default_headers(self):
        return {
            "Authorization": "OAuth {}".format(self.token),
        }

    @staticmethod
    def _verify_response(response):
        if response.status_code != 200:
            raise LogrepError(
                "YQL returned error: {}".format(response.text)
            )

    def _retry_request(self, request):
        retry_count = 5
        prepared = request.prepare()
        for i in xrange(retry_count):
            s = self.requests.Session()
            try:
                res = s.send(prepared)
            except self.requests.RequestException as e:
                if i == retry_count - 1:
                    raise
                logging.warning("going to retry failed request {}: {}".format(request, str(e)))
                time.sleep(1)
                continue

            if res.status_code == 200:
                return res
            elif res.status_code % 100 == 5:
                if i == retry_count -1:
                    return res
                logging.warning("going to retry failed request {}: {}".format(request, response.text))
                time.sleep(1)
                continue

    def start(self):
        if self.id is not None:
            raise RuntimeError("Request is already started")

        headers = self._get_default_headers()
        headers["Content-Type"] = "application/json"

        data = json.dumps({
            "content": self.query,
            "action": "RUN",
            "type": self.type,
        })

        req = self.requests.Request(
            "POST",
            "https://yql.yandex.net/api/v2/operations",
            headers=headers,
            data=data
        )

        res = self._retry_request(req)
        self._verify_response(res)

        json_res = res.json()
        self.id = json_res["id"]
        self.status = json_res["status"]
        self.start_time = datetime.datetime.now()

    def get_url(self):
        return "https://yql.yandex-team.ru/Operations/" + self.id

    def interactive_wait(self):
        try:
            while True:
                delta = datetime.datetime.now() - self.start_time
                sys.stderr.write("\rOperation {} is running for {} seconds...".format(self.get_url(), delta.seconds))
                sys.stderr.flush()
                completed = self.check_completed()
                if completed:
                    break
                time.sleep(1)
        finally:
            sys.stderr.write("\n")

    def check_completed(self):
        if self.id is None:
            raise RuntimeError("Operation is not running")

        headers = self._get_default_headers()

        req = self.requests.Request(
            "GET",
            "https://yql.yandex.net/api/v2/operations/" + self.id,
            headers=headers,
        )
        res = self._retry_request(req)
        self._verify_response(res)
        json_res = res.json()
        self.id = json_res["id"]
        self.status = json_res["status"]

        if self.status in ["PENDING", "RUNNING"]:
            return False
        if self.status == "COMPLETED":
            return True

        raise LogrepError(
            "Operation has bad status: {}\n"
            "  {}\n"
            .format(self.status, self.get_url())
        )

    def iter_results(self):
        if self.status != "COMPLETED":
            raise RuntimeError("Operation is not completed")

        headers = self._get_default_headers()
        res = self.requests.get(
            "https://yql.yandex.net/api/v2/operations/" + self.id + "/results_data?format=json&write_index=0",
            headers=headers,
            stream=True,
        )
        self._verify_response(res)

        for line in res.iter_lines():
            j = json.loads(line)
            yield ChytGrepResult(
                timestamp=j["timestamp"],
                log_level=j["log_level"],
                log_category=j["log_category"],
                log_message=j["log_message"],
                node=extract_node_from_source_uri(j["source_uri"]))


def yql_grep(tables, cluster, pattern, use_chyt=False):
    if use_chyt:
        query = get_chyt_query(tables, cluster, pattern)
        flavour = "CLICKHOUSE"
    else:
        query = get_yql_query(tables, cluster, pattern)
        flavour = "SQLv1"

    logging.info("Starting YQL operation")
    yql_operation = YqlOperation(get_yql_token(), query, flavour=flavour)

    yql_operation.start()

    logging.info("YQL operation started: {}".format(yql_operation.get_url()))

    yql_operation.interactive_wait()

    for r in yql_operation.iter_results():
        yield r
