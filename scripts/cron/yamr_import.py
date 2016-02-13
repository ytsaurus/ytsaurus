#!/usr/bin/env python

from yt.tools.yamr import Yamr
from yt.common import update
from yt.wrapper.errors import YtResponseError
from yt.wrapper.http import get_token
from yt.wrapper.client import Yt
import yt.packages.requests as requests
import yt.logger as logger

import remclient as rem

import os
import simplejson as json
import calendar
from datetime import date, datetime, timedelta

TASKS_LIMIT = 50

def process_day_logs(yt_client, source_pattern, destination_pattern, period, import_callback, remove_callback, exists_callback):
    if period is None:
        count = 15
    else:
        count = period + 100

    for i in xrange(count):
        table_date = date.today() - timedelta(days=i)
        def to_name(pattern):
            return pattern.format(table_date.strftime("%Y%m%d"))

        src = to_name(source_pattern)
        dst = to_name(destination_pattern)
        if period is not None and i >= period:
            remove_callback(dst)
        else:
            if not yt_client.exists(dst) or yt_client.get(dst + "/@row_count") == 0:
                import_callback(src, dst)
            else:
                exists_callback(dst)

def process_hour_logs(yt_client, source_pattern, destination_pattern, period, import_callback, remove_callback, exists_callback):
    for i in xrange(period + 24 * 30):
        for minutes in [0, 30]:
            now = datetime.utcnow()
            rounded_now = datetime(now.year, now.month, now.day, now.hour) - timedelta(hours=i, minutes=minutes)
            name = calendar.timegm(rounded_now.timetuple())

            src = source_pattern.format(name)
            dst = destination_pattern.format(name)
            if i >= period:
                remove_callback(dst)
            else:
                if not yt_client.exists(dst) or yt_client.get(dst + "/@row_count") == 0:
                    import_callback(src, dst)
                else:
                    exists_callback(dst)

class YamrExistanceChecker(object):
    def __init__(self, client):
        self.client = client
        self.requested_prefixes = []
        self.cache = {}

    def check(self, table):
        found = False
        for prefix in self.requested_prefixes:
            if table.startswith(prefix):
                found = True
                break
        if not found:
            path = table
            if "/" in table:
                path = table.rsplit("/", 1)[0] + "/"

            result = self.client.list(path)
            self.requested_prefixes.append(path)
            for obj in result:
                self.cache[obj["name"]] = obj

        return table in self.cache

class TaskRunner(object):
    def __init__(self, yamr_clients, destination_cluster_name, tm_url, copy_pool=None, postprocess_pool=None):
        self.copy_pool = copy_pool
        self.postprocess_pool = postprocess_pool

        self.yamr_clients = yamr_clients

        self.existance_checkers = []
        for client in self.yamr_clients:
            self.existance_checkers.append(YamrExistanceChecker(client))

        self.destination_cluster_name = destination_cluster_name
        self.yt_client = Yt(proxy=destination_cluster_name, token=os.environ["YT_TOKEN"])
        self.tm_url = tm_url

        tasks = self._get_transfer_manager_tasks(self.tm_url, params={
            "fields[]": [
                "id",
                "state",
                "destination_table",
                "destination_cluster"
            ]})

        self.tables_in_progress = dict(
            [(task.get("destination_table"), task["id"]) for task in tasks
             if (task["destination_cluster"] == destination_cluster_name and
                 task["state"] in ["pending", "running"])])

        self.copying_tables_count = len(self.tables_in_progress)

    def _get_transfer_manager_tasks(self, tm_url, params):
        return requests.get("http://{0}/tasks/".format(tm_url), params=params).json()

    def _start_transfer_manager_task(self, yamr_client, src, dst, params=None):
        if self.copying_tables_count >= TASKS_LIMIT:
            logger.info("Skipping import of %s (%s) to %s (%s), since too many (>=%d) running copy tasks",
                        src, yamr_client.name, dst, self.destination_cluster_name, self.copying_tables_count)
            return

        logger.info("Importing %s (%s) to %s (%s)", src, yamr_client.name, dst, self.destination_cluster_name)
        if params is None:
            params = {}

        default_params = {
            "source_cluster": yamr_client.name,
            "destination_cluster": self.destination_cluster_name,
            "source_table": src,
            "destination_table": dst,
            "mr_user": "userdata",
            "destination_compression_codec": "gzip_best_compression",
            "destination_erasure_codec": "lrc_12_2_2"
        }
        if self.postprocess_pool is not None:
            params["postprocess_spec"] = {"pool": self.postprocess_pool}
        if self.copy_pool is not None:
            params["pool"] = self.copy_pool
            params["copy_spec"] = {"pool": self.copy_pool}

        headers = {
            "Content-Type": "application/json",
            "Authorization": "OAuth " + get_token(self.yt_client)
        }

        params = update(default_params, params)

        rsp = requests.post("http://{0}/tasks/".format(self.tm_url), data=json.dumps(params), headers=headers)
        if not str(rsp.status_code).startswith("2"):
            logger.info("Task result: " + rsp.content)
            message = rsp.json()["message"]
            if "Precheck" in message and "failed" in message:
                return
            rsp.raise_for_status()

        self.copying_tables_count += 1

    def run(self, source_table, destination_table, params):
        logger.info("Checking import from %s to %s", source_table, destination_table)
        for yamr_client, existance_checker in zip(self.yamr_clients, self.existance_checkers):
            if destination_table in self.tables_in_progress:
                logger.info("Table %s is copying by task %s", destination_table, self.tables_in_progress[destination_table])
                continue
            if not existance_checker.check(source_table):
                continue
            if self.yt_client.exists(destination_table) and self.yt_client.row_count(destination_table) > 0:
                continue

            self._start_transfer_manager_task(yamr_client, source_table, destination_table, params=params)
            break


def create_task_runner(destination_cluster_name, tm_url="transfer-manager.yt.yandex.net/api/v1"):
    def check_yamr(client):
        try:
            client.list("tmp")
            return True
        except Exception:
            return False
    yamr_cluster_names = ["cedar", "sakura"]
    yamr_clusters = [Yamr(binary="/Berkanavt/bin/mapreduce-dev",
                          name=name,
                          server=name + "00.search.yandex.net",
                          server_port="8013",
                          http_port="13013",
                          timeout=180.0
                     ) for name in yamr_cluster_names]
    yamr_clusters = filter(check_yamr, yamr_clusters)
    return TaskRunner(yamr_clusters, destination_cluster_name, tm_url=tm_url, copy_pool="yamr_copy", postprocess_pool="yamr_postprocess")

def get_dash_date_table(table):
    date = table[-8:]
    new_date = date[:4] + "-" + date[4:6] + "-" + date[6:]
    return table[:-8] + new_date

def remove(table, yt_client, remove_link):
    def do_remove(table):
        if yt_client.exists(table):
            logger.info("Removing %s", table)
            try:
                yt_client.remove(table, force=True)
            except YtResponseError as err:
                if not err.is_concurrent_transaction_lock_conflict():
                    raise

    do_remove(table)
    if remove_link:
        do_remove(get_dash_date_table(table))

def is_processed(table, yt_client):
    return yt_client.exists(table + "/@processed") and yt_client.get(table + "/@processed")

def set_processed(table, yt_client):
    yt_client.set(table + "/@processed", True)

def make_link(table, yt_client):
    link_table = get_dash_date_table(table)
    if not yt_client.exists(link_table) and yt_client.exists(link_table + "&"):
        yt_client.remove(link_table)
    if not yt_client.exists(link_table):
        yt_client.link(table, link_table)

def set_ydf_attribute(table, value, yt_client):
    if value is not None:
        yt_client.set(table + "/@_yql_read_udf", value)
        for item in ["key", "subkey", "value"]:
            yt_client.set(table + "/@_yql_{0}_meta".format(item),
                          {"Type": ["DataType", "String"], "Name": item})

def set_format_attribute(table, value, yt_client):
    if value is not None:
        yt_client.set(table + "/@_format", value)

def notify_rem(rem_connection, dst, cluster_name):
    prefix = "//userdata/"
    if dst.startswith(prefix):
        dst = dst[len(prefix):]
    rem_connection.Tag("export_to_yt_{0}_{1}".format(cluster_name, dst.replace("/", "_"))).Set()

class Importer(object):
    def __init__(self, proxy):
        self.proxy = proxy
        self.cluster_name = proxy.split(".")[0]
        self.yt_client = Yt(proxy=proxy, token=os.environ["YT_TOKEN"])
        self.task_runner = create_task_runner(self.cluster_name)
        self.rem_connection = rem.Connector("http://veles02:8104/")

    def process_log(self, type, source_pattern, destination_pattern, period, ydf_attribute=None, format_attribute=None, link=None):
        def postprocess(dst, link):
            if is_processed(dst, self.yt_client):
                return
            logger.info("Postprocess table %s", dst)
            if link:
                make_link(dst, self.yt_client)
            notify_rem(self.rem_connection, dst, self.proxy)
            set_ydf_attribute(dst, ydf_attribute, self.yt_client)
            set_format_attribute(dst, format_attribute, self.yt_client)
            set_processed(dst, self.yt_client)

        if destination_pattern is None:
            destination_pattern = source_pattern
        destination_pattern = os.path.join("//userdata", destination_pattern)

        logger.info("Processing import from %s to %s of type '%s' by period %s",
                    source_pattern, destination_pattern, type, str(period) if period else "inf")

        if type == "day":
            if link is None:
                link = True
            process_day_logs(self.yt_client, source_pattern, destination_pattern, period,
                             import_callback=lambda src, dst: self.task_runner.run(src, dst, None),
                             remove_callback=lambda table: remove(table, self.yt_client, remove_link=True),
                             exists_callback=lambda table: postprocess(table, link=link))
        if type == "hour":
            if link is None:
                link = False
            process_hour_logs(self.yt_client, source_pattern, destination_pattern, period,
                              import_callback=lambda src, dst: \
                                    self.task_runner.run(src, dst, params=\
                                        {"destination_compression_codec": "gzip_best_compression",
                                         "destination_erasure_codec": "lrc_12_2_2"}),
                              remove_callback=lambda table: remove(table, self.yt_client, remove_link=False),
                              exists_callback=lambda table: postprocess(table, link=link))
