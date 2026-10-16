from yt.wrapper.common import chunk_iter_list
from yt.wrapper.retries import Retrier

from yt.packages.requests import HTTPError, ConnectionError, Timeout
import yt.packages.requests as requests

from socket import error as SocketError
import datetime
import logging
import os
import simplejson as json

import sys


SOLOMON_PUSH_URL = os.environ.get("SOLOMON_PUSH_URL", "https://api.solomon.search.yandex.net/api/v2/push")

PUSH_RETRY_COUNT = 5
PUSH_REQUEST_TIMEOUT = 60000

DEFAULT_SOLOMON_PUSH_CHUNK_SIZE = 2000


class SolomonServerTimedOut(Exception):
    pass


class PushRequestRetrier(Retrier):
    def __init__(self, url, data, params, headers, files=None, is_solomon_push=False):
        retry_config = {
            "enable": True,
            "count": PUSH_RETRY_COUNT,
            "backoff": {"policy": "constant_time", "constant_time": 3000},
        }

        self.url = url
        self.data = data
        self.params = params
        self.files = files
        self.headers = headers

        self.is_solomon_push = is_solomon_push

        exceptions = [Timeout, ConnectionError, HTTPError, SocketError]
        if is_solomon_push:
            exceptions.append(SolomonServerTimedOut)

        super(PushRequestRetrier, self).__init__(
            retry_config, timeout=PUSH_REQUEST_TIMEOUT, exceptions=tuple(exceptions)
        )

    def action(self):
        r = requests.post(
            self.url,
            data=self.data,
            params=self.params,
            files=self.files,
            headers=self.headers,
            timeout=self.timeout / 1000.0,
            verify=False,
        )

        if not str(r.status_code).startswith("2"):
            logging.warning(
                "HTTP POST request failed (url: %s, rsp: %s)", self.url, r.text
            )
            if r.status_code == 504 and self.is_solomon_push:
                raise SolomonServerTimedOut()
            else:
                r.raise_for_status()

    def except_action(self, exception, _):
        logging.warning(
            'HTTP POST request (url: %s) failed with error %s, message: "%s"',
            self.url,
            str(type(exception)),
            str(exception),
        )


def get_current_timestamp():
    if sys.version_info.major == 2:
        return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds())
    else:
        return int(datetime.datetime.now(datetime.UTC).timestamp())


def push_cluster_data_to_solomon(cluster, service, sensors, project="yt", chunk_size=None):
    if chunk_size is None:
        chunk_size = DEFAULT_SOLOMON_PUSH_CHUNK_SIZE

    utc_now_ts = get_current_timestamp()
    for sensor in sensors:
        if "ts" not in sensor:
            sensor["ts"] = utc_now_ts

    data = {"commonLabels": {"host": "none"}}
    params = {
        "project": project,
        "cluster": cluster,
        "service": service,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": "OAuth " + _get_solomon_token(),
    }

    for sensors_chunk in chunk_iter_list(sensors, chunk_size):
        data["sensors"] = sensors_chunk
        retrier = PushRequestRetrier(
            SOLOMON_PUSH_URL, json.dumps(data), params, headers, is_solomon_push=True
        )
        retrier.run()


def _get_solomon_token():
    candidate_paths = [
        os.environ.get("SOLOMON_TOKEN_PATH"),
        os.path.expanduser("~/.solomon/token"),
    ]
    candidate_paths = [path for path in candidate_paths if path]

    for path in candidate_paths:
        if os.path.exists(path):
            with open(path, "r") as f:
                return f.read().strip()

    if os.environ.get("SOLOMON_TOKEN"):
        return os.environ.get("SOLOMON_TOKEN")

    raise RuntimeError(
        "Failed to find Solomon token (tried paths: {})".format(candidate_paths)
    )
