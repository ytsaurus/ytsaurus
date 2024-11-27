#!/usr/bin/env python3
# coding=utf-8
import time
import yt.packages.requests as requests
from yt_odin_checks.lib.check_runner import main

TIMEOUT = 30


class CheckData:
    def __init__(self, state, message, time_of_last_check):
        self.state = state
        self.message = message
        self.time_of_last_check = time_of_last_check

    @classmethod
    def from_json(cls, data_json):
        return cls(
            data_json["state"], data_json["message"], data_json["time_of_last_check"]
        )

    @classmethod
    def get_default_ok_state(cls):
        return cls("OK", "OK", 0.0)

    def to_json(self):
        return {
            "state": self.state,
            "message": self.message,
            "time_of_last_check": self.time_of_last_check,
        }


def need_to_update_state(last_check_data):
    # type: (CheckData) -> bool
    curr_time = time.time()
    hour = 60 * 60
    return curr_time - last_check_data.time_of_last_check >= hour


def discover_versions(yt_client, yt_token, logger):
    # type: (YtClient, str, logging.Logger) -> typing.List[dict] or None
    try:
        proxy = yt_client.config["proxy"]["url"]
        url = f"https://{proxy}/internal/discover_versions/v2"
        resp = requests.get(
            url, headers={"Authorization": "OAuth " + yt_token}, timeout=TIMEOUT
        )
        resp.raise_for_status()
        resp_json = resp.json()
    except Exception as e:
        logger.warning(f"Failed to discover local versions with error {e}")
        return None

    local_versions = []
    for binary in resp_json["details"]:
        if (
            binary["type"] in ["primary_master", "controller_agent", "scheduler"]
            and "version" in binary
            and "-local-" in binary["version"]
        ):
            local_versions.append(
                {
                    "address": binary["address"],
                    "version": binary["version"],
                }
            )
    return local_versions


def check_versions(versions):
    # type: (typing.List[dict]) -> CheckData
    curr_time = time.time()
    if len(versions) > 0:
        local_versions = "\n".join([f"{version['address']}:{version['version']}" for version in versions])
        return CheckData(
            "CRIT", f"Local binaries found: {local_versions}", curr_time
        )
    return CheckData("OK", "OK", curr_time)


def get_current_state(yt_client, yt_token, logger):
    # type: (YtClient, str, logging.Logger) -> CheckData or None
    versions = discover_versions(yt_client, yt_token, logger)
    if versions is None:
        return None
    return check_versions(versions)


def get_info_about_last_check(yt_client, doc_path):
    # type: (YtClient, str) -> CheckData
    if yt_client.exists(doc_path):
        return CheckData.from_json(yt_client.get(doc_path))
    return CheckData.get_default_ok_state()


def update_current_state(yt_client, logger, doc_path, check_data):
    # type: (YtClient, logging.Logger, str, CheckData) -> None
    if not yt_client.exists(doc_path):
        yt_client.create("document", doc_path)

    try:
        yt_client.set(doc_path, check_data.to_json())
    except Exception as e:
        logger.warning(f"Failed to write info to file {doc_path} with error: {e}")


def run_check(secrets, yt_client, logger, states):
    current_state_path = "//sys/admin/odin/local_binary"
    yt_token = secrets["yt_token"]
    check_data = get_info_about_last_check(yt_client, current_state_path)

    if need_to_update_state(check_data):
        check_data = get_current_state(yt_client, yt_token, logger)
        if check_data is None:
            return states.PARTIALLY_AVAILABLE_STATE, "Failed to discover local versions"
        update_current_state(yt_client, logger, current_state_path, check_data)

    logger.info(check_data.message)
    if check_data.state == "CRIT":
        return states.UNAVAILABLE_STATE, check_data.message
    return states.FULLY_AVAILABLE_STATE, "OK"


if __name__ == "__main__":
    main(run_check)
