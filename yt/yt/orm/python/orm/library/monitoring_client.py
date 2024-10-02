import yt.logger as logger
import yt.packages.requests as requests
import yt.yson as yson

import json


class OrmMonitoringClient(object):
    def __init__(self, address):
        self._address = address

    def _dumps_params(self, params):
        return {key: yson.dumps(value) for key, value in params.items()}

    def _check_response(self, path, params, rsp):
        if rsp.status_code != 200:
            logger.warning(
                "Request failed (path: %s, params: %s, status_code: %d, body: %s, headers: %s)",
                path,
                params,
                rsp.status_code,
                rsp.content,
                rsp.headers,
            )
        rsp.raise_for_status()

    def get(self, path, service="/orchid", timeout=20.0, **kwargs):
        assert path.startswith("/"), "Path must start with slash"

        params = self._dumps_params(kwargs)
        params["verb"] = "get"

        url = "http://{}{}{}".format(self._address, service, path)
        rsp = requests.get(url, timeout=timeout, params=params)
        self._check_response(path, params, rsp)

        if rsp.text:
            return json.loads(rsp.text)

    def list(self, path):
        assert path.startswith("/"), "Path must start with slash"

        params = {"verb": "list"}

        url = "http://{}/orchid{}".format(self._address, path)
        rsp = requests.get(url, params=params)
        self._check_response(path, params, rsp)

        return json.loads(rsp.text)

    def get_sensor(self, name, **kwargs):
        name = "yt/" + name
        return self.get("/sensors", name=name, **kwargs)

    def list_sensors(self):
        yt_prefix = "yt/"

        sensors = self.list("/sensors")
        for sensor in sensors:
            assert sensor.startswith(yt_prefix)

        return [sensor[len(yt_prefix):] for sensor in sensors]
