import requests

import yt.common


def read_value(monitoring_url, name, tags):
    params = tags.copy()
    params["sensor"] = name

    rsp = requests.get(monitoring_url + "/test", params=params)
    if rsp.status_code != 200:
        raise yt.common.YtError(
            "Sensor read failed",
            inner_errors=[yt.common.YtError(rsp.text)],
            attributes={
                "sensor": name,
                "tags": tags,
            })

    return rsp.json()
