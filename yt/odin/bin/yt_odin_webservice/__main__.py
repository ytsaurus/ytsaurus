#!/usr/bin/env python3

from yt_odin.odinserver.common import iso_time_to_timestamp, SECONDS_IN_MINUTE
from yt_odin.storage.db import get_cluster_client_factory_from_db_config, create_yt_table_client_from_config
from yt_odin.logserver import (
    is_state_ok,
    UNAVAILABLE_STATE,
    FULLY_AVAILABLE_STATE,
    TERMINATED_STATE
)

from yt.flask_helpers import process_gzip

from yt.common import update

from cheroot import wsgi as wsgiserver
from flask import Flask, make_response, request, jsonify
import dateutil

import re
import json
import time
import argparse
import logging
from copy import deepcopy
import uuid
import calendar
import datetime
import traceback

app = Flask("yt_odin_webservice")

DB_TABLE_CLIENT = None
DB_TABLE_CLIENTS_FOR_CLUSTERS = {}
SERVICES = []
SOLOMON_LAG = 600  # seconds


def status_to_string(status):
    if status == UNAVAILABLE_STATE:
        return "unavailable"
    if status == FULLY_AVAILABLE_STATE:
        return "available"
    if 0.01 < status < 0.99:
        return "partially_available"
    if status == TERMINATED_STATE:
        return "timed_out"
    return "unknown"


def build_response(*args, **kwargs):
    response = make_response(*args, **kwargs)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


def strip_cluster_name(name):
    SUFFIX = ".yt.yandex.net"
    if name.endswith(SUFFIX):
        return name[:-len(SUFFIX)]
    return name


def extract_all_records(start_timestamp, stop_timestamp):
    services = [service["name"] for service in SERVICES]
    clusters = DB_TABLE_CLIENTS_FOR_CLUSTERS.keys()
    return DB_TABLE_CLIENT.get_records(clusters, services, start_timestamp, stop_timestamp)


def extract_records(cluster, service, start_timestamp, stop_timestamp):
    db_client = DB_TABLE_CLIENTS_FOR_CLUSTERS[cluster]
    return db_client.get_records(service, start_timestamp, stop_timestamp)


def extract_statuses_and_messages(request_cluster, start_timestamp, stop_timestamp,
                                  request_target, return_state_without_message):
    db_response = extract_records(request_cluster, request_target, start_timestamp, stop_timestamp)

    def get_minute(timestamp):
        return "at_{0:0>5}".format(int((timestamp - start_timestamp) / 60))

    def get_value(status, message):
        if return_state_without_message:
            return status
        else:
            return {"state": status_to_string(status), "message": message}

    return {get_minute(record.timestamp): get_value(record.state, record.messages)
            for record in db_response}


def parse_solomon_timedelta(string):
    regex = re.compile(r'((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')
    parts = regex.match(string)
    if not parts:
        raise ValueError("Invalid Solomon timedelta: {0}".format(string))
    parts = parts.groupdict()
    time_params = {}
    for name, param in parts.items():
        if param:
            time_params[name] = int(param)
    return datetime.timedelta(**time_params)


def create_solomon_duration_sensors(records):
    sensors = []
    for record in records:
        if record.duration is not None:
            sensors.append(dict(
                labels=dict(
                    sensor="duration",
                    check=record.service,
                    proxy=record.cluster,
                ),
                ts=record.timestamp,
                kind="IGAUGE",
                value=record.duration,
            ))
    return sensors


def create_solomon_timed_out_sensors(records):
    sensors = []
    for record in records:
        value = 1 if record.duration is None else 0
        sensors.append(dict(
            labels=dict(
                sensor="timed_out",
                check=record.service,
                proxy=record.cluster,
            ),
            ts=record.timestamp,
            kind="IGAUGE",
            value=value,
        ))
    return sensors


@app.route("/solomon")
def solomon_duration():
    # NB: we assume `now` is a UTC datetime.
    now = dateutil.parser.parse(request.args.get("now"))
    period = parse_solomon_timedelta(request.args.get("period"))
    # NB: shift `now` into past to make sure that all the checks have completed.
    now -= datetime.timedelta(seconds=SOLOMON_LAG)
    start_timestamp = int(calendar.timegm((now - period).utctimetuple()))
    stop_timestamp = int(calendar.timegm(now.utctimetuple()))
    # NB: do not include `start_timestamp` to avoid duplicating records.
    db_records = extract_all_records(start_timestamp + 1, stop_timestamp)
    duration_sensors = create_solomon_duration_sensors(db_records)
    timed_out_sensors = create_solomon_timed_out_sensors(db_records)
    return jsonify({"sensors": duration_sensors + timed_out_sensors})


@app.route("/exists/<string:cluster>")
def exists(cluster):
    return build_response(json.dumps(strip_cluster_name(cluster) in DB_TABLE_CLIENTS_FOR_CLUSTERS), 202)


@app.route("/availability/<string:cluster>")
@process_gzip
def availability(cluster):
    start_timestamp = iso_time_to_timestamp(request.args.get("start_time"))
    end_timestamp = iso_time_to_timestamp(request.args.get("end_time"))
    metric = request.args.get("metric", "sort_result")

    return_state_without_message = bool(int(request.args.get("return_state_without_message", "1")))

    cluster = strip_cluster_name(cluster)

    statuses_and_messages = extract_statuses_and_messages(
        cluster,
        start_timestamp,
        end_timestamp,
        metric,
        return_state_without_message)

    statuses_and_messages["metric"] = metric

    json_dump = json.dumps(statuses_and_messages, sort_keys=True)
    return build_response(json_dump, 202)


@app.route("/service_list")
def service_list():
    return build_response(json.dumps(SERVICES), 202)


@app.route("/availability_stat/<string:cluster>")
def availability_stat(cluster):
    period = int(request.args.get("period", 15))
    target = request.args.get("target", "sort_result")

    # COMPAT: Web interface uses old names.
    if target == "YtMasterAvailability":
        target = "master"

    cluster = strip_cluster_name(cluster)

    stop_timestamp = int(time.time())
    start_timestamp = stop_timestamp - SECONDS_IN_MINUTE * (1 + period)

    db_response = extract_records(cluster, target, start_timestamp, stop_timestamp)
    statuses = [is_state_ok(record.state) for record in db_response]
    return build_response(str(min(1.0, float(sum(statuses)) / period)), 202)


@app.route("/is_alive")
def is_alive():
    return build_response(json.dumps(DB_TABLE_CLIENT.is_alive()), 202)


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def catch_all(path):
    return "No service at root path"


@app.before_request
def before_request():
    request_id = uuid.uuid4().hex[:8]
    request.id = request_id
    request.start_time = time.time()

    app.logger.info(
        "Request received (RequestId: %s, Method: %s, Path: %s, RemoteAddress: %s)",
        request.id,
        request.method,
        request.full_path,
        request.remote_addr)


@app.after_request
def after_request(response):
    app.logger.info("Request processed (RequestId: %s, Status: %d, Duration: %.4f)",
                    request.id, response.status_code, time.time() - request.start_time)

    return response


@app.errorhandler(Exception)
def handle_error(exception):
    error_str = traceback.format_exc().replace("\n", "\\n")

    app.logger.warning("Request failed (RequestId: %s, Error: %s)",
                       request.id, error_str)
    return "Internal server error", 500


def main():
    parser = argparse.ArgumentParser(description="web service of Odin")
    parser.add_argument("--config", required=True, help="path to config file (file in json format)")

    args = parser.parse_args()
    config = json.load(open(args.config))

    global DB_TABLE_CLIENT
    DB_TABLE_CLIENT = create_yt_table_client_from_config(config["db_config"])

    global DB_TABLE_CLIENTS_FOR_CLUSTERS
    clusters = config["clusters"]
    for cluster in clusters:
        db_config = update(deepcopy(config["db_config"]), clusters[cluster]["db_config"])
        DB_TABLE_CLIENTS_FOR_CLUSTERS[cluster] = get_cluster_client_factory_from_db_config(db_config)()

    global SERVICES
    SERVICES = config["services"]

    if "logging" in config:
        handler = logging.handlers.WatchedFileHandler(config["logging"]["filename"])
        handler.setFormatter(logging.Formatter("%(asctime)-15s\t%(levelname)s\t%(message)s"))
        app.logger.handlers = [handler]
        app.logger.setLevel(logging.INFO)
        logging.getLogger("werkzeug").setLevel(logging.ERROR)

        yt_logger = logging.getLogger("Yt")
        yt_logger.handlers = [handler]
        yt_logger.setLevel(logging.INFO)

    if config["debug"]:
        app.run(port=config["port"], debug=config["debug"], host=config["host"])
    else:
        dispatcher = wsgiserver.WSGIPathInfoDispatcher({'/': app})
        server = wsgiserver.WSGIServer(
            (config["host"], config["port"]),
            dispatcher,
            numthreads=config["thread_count"])
        try:
            server.start()
        except KeyboardInterrupt:
            server.stop()


if __name__ == "__main__":
    main()
