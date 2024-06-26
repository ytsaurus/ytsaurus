from .common import get_user_agent, YtError
from .constants import DEFAULT_HOST_SUFFIX
from .http_driver import TokenAuth
from .http_helpers import get_token, format_logging_params

import yt.logger as logger
import yt.wrapper.yson as yson
import yt.packages.requests as requests
import yt.wrapper as yt

import os
import time


def get_full_ctl_address(address, family, stage):
    family_upper = family.upper()

    if not address:
        address = os.getenv("{}_CTL_ADDRESS".format(family_upper))
    if not address:
        address = os.getenv("STRAWBERRY_CTL_ADDRESS")
    if not address:
        address = yt.config["strawberry_ctl_address"]

    address = address.format(stage=stage, family=family, host_suffix=DEFAULT_HOST_SUFFIX)

    if not address.startswith("http://") and not address.startswith("https://"):
        address = "http://" + address

    return address


def get_cluster_name(cluster_name):
    if not cluster_name:
        cluster_name = yt.config["strawberry_cluster_name"]
    if not cluster_name:
        cluster_name = yt.config["proxy"]["url"]

    return cluster_name


def describe_api(address, family, stage):
    address = get_full_ctl_address(address, family, stage)

    url = address + "/describe"
    headers = {
        "User-Agent": get_user_agent(),
    }

    logging_params = {
        "headers": headers,
    }
    logger.debug("Perform HTTP GET request %s (%s)", url, format_logging_params(logging_params))

    response = requests.get(address + "/describe")

    logging_params = {
        "headers": dict(response.headers),
        "status_code": response.status_code,
        "body": response.content,
    }
    logger.debug("Response received (%s)", format_logging_params(logging_params))

    if response.status_code != 200:
        raise YtError("Bad response from controller service", attributes={
            "status_code": response.status_code,
            "response_body": response.content})

    return yson.loads(response.content)


def make_request(command_name, params, address, family, stage, cluster_name, unparsed=False):
    address = get_full_ctl_address(address, family, stage)

    url = "{}/{}/{}".format(address, cluster_name, command_name)
    data = yson.dumps({"params": params, "unparsed": unparsed})
    auth = TokenAuth(get_token())
    headers = {
        "User-Agent": get_user_agent(),
        "Content-Type": "application/yson"
    }

    test_user = os.getenv("YT_TEST_USER")
    if test_user:
        headers["X-YT-TestUser"] = test_user

    logging_params = {
        "headers": headers,
        "params": params,
    }
    logger.debug("Perform HTTP POST request %s (%s)", url, format_logging_params(logging_params))

    response = requests.post(url, data=data, auth=auth, headers=headers)

    logging_params = {
        "headers": dict(response.headers),
        "status_code": response.status_code,
        "body": response.content,
    }
    logger.debug("Response received (%s)", format_logging_params(logging_params))

    if response.status_code == 403:
        raise YtError("Authorization failed; check that your yt token is valid", attributes={
            "response_body": response.content})

    if response.status_code not in [200, 400]:
        raise YtError("Bad response from controller service", attributes={
            "status_code": response.status_code,
            "response_body": response.content})

    return yson.loads(response.content)


def make_request_generator(command_name, params, address, family, stage, cluster_name, unparsed=False):
    while True:
        response = make_request(
            command_name=command_name,
            params=params,
            address=address,
            family=family,
            stage=stage,
            cluster_name=cluster_name,
            unparsed=unparsed)

        yield response

        if "wait_ready" in response:
            command_name = response["wait_ready"]["command_name"]
            params = response["wait_ready"]["params"]
            unparsed = False
            time.sleep(response["wait_ready"]["backoff"])

        else:
            break


def _get_result_or_raise(response, family):
    if "result" in response:
        return response["result"]

    if "code" in response and "message" in response:
        raise YtError(response["message"], attributes={"error": response})

    # TODO(gudqeit): "error" and "to_print" fields are deprecated and are no longer in use.
    # Remove code below when all controllers are updated.
    if "error" in response:
        if "to_print" in response:
            raise YtError(response["to_print"], attributes={"error": response["error"]})
        else:
            raise YtError("Error was received from {} controller API".format(family),
                          attributes={"error": response["error"]})

    return response


class StrawberryClient(object):
    def __init__(self, address=None, cluster_name=None, family=None, stage="production"):
        self.address = address
        self.cluster_name = get_cluster_name(cluster_name)
        self.family = family
        self.stage = stage

    def make_controller_request(self, command_name, params):
        for response in make_request_generator(
                command_name=command_name,
                params=params,
                address=self.address,
                family=self.family,
                stage=self.stage,
                cluster_name=self.cluster_name):
            result = _get_result_or_raise(response, self.family)
        return result

    def list(self):
        return self.make_controller_request("list", params={})

    def create(self, alias):
        self.make_controller_request("create", params={"alias": alias})

    def remove(self, alias):
        self.make_controller_request("remove", params={"alias": alias})

    def exists(self, alias):
        return self.make_controller_request("exists", params={"alias": alias})

    def status(self, alias):
        return self.make_controller_request("status", params={"alias": alias})

    def get_option(self, alias, key):
        return self.make_controller_request("get_option", params={
            "alias": alias,
            "key": key,
        })

    def set_option(self, alias, key, value):
        self.make_controller_request("set_option", params={
            "alias": alias,
            "key": key,
            "value": value,
        })

    def remove_option(self, alias, key):
        self.make_controller_request("remove_option", params={
            "alias": alias,
            "key": key,
        })

    def get_speclet(self, alias):
        return self.make_controller_request("get_speclet", params={"alias": alias})

    def set_speclet(self, alias, speclet):
        self.make_controller_request("set_speclet", params={
            "alias": alias,
            "speclet": speclet,
        })

    def start(self, alias, untracked=False):
        return self.make_controller_request("start", params={
            "alias": alias,
            "untracked": untracked,
        })

    def stop(self, alias):
        self.make_controller_request("stop", params={"alias": alias})
