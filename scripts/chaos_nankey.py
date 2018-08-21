#!/usr/bin/python
#
# Chaos monkey script for nanny services accepts several group of services:
#
# ./chaos_monkey [--cleanup] {SERVICE_GROUP_NAME1=SERVICE_GROUP1; SERVICE_GROUP_NAME2=SERVICE_GROUP2 ... }
#
#  SERVICE_GROUP is yson-map service description:
#     {
#          "services" - list of nanny service names
#          "offline" - maximum number of offline instances
#          "sleep" - number of seconds between instance activation/deactivation
#          "regexp" - regular expression which will be applied to instance slot
#     }
#
#  Example:
#  ./chaos_nankey.py '{nodes={services=[man_yt_socrates_nodes_tablet;man_yt_socrates_nodes;man_yt_socrates_nodes_rootfs];sleep=60;offline=2};masters={services=[man_yt_socrates_masters];offline=1;sleep=300;regexp="m[0][1-3]"}}'
#

import yt.yson as yson
import requests
import argparse
import json
import re
from urlparse import urljoin

import sys
import os
import random
import functools

import logging
import time

import urllib3
urllib3.disable_warnings()
VERIFY_SSL=False
#logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(format='%(asctime)s %(name)s: %(message)s', level=logging.INFO)

def wait(predicate):
    while not predicate():
        time.sleep(2)

def retry(func=None, tries=12, delay=5):
    if func is None:
        return functools.partial(retry, tries=tries, delay=delay)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        exception = None
        for i in range(tries):
            try:
                return func(*args, **kwargs)
            except Exception as ex:
                time.sleep(delay)
                exception = ex
        raise exception

    return wrapper

class NannyInstance:
    def __init__(self, nanny, instance_url, descr, logger):
        self._nanny = nanny
        self._instance_url = instance_url
        self._descr = descr
        self._logger = logger

    def __repr__(self):
        return "<NannyInstance {0}>".format(self._instance_url)

    def get_slot(self):
        return self._descr["slot"]

    def get_target_state(self):
        return self._descr["target_state"]
    def get_current_state(self):
        return self._descr["current_state"]

    def set_target_state(self, target_state):
        self._logger.info("Instance {0} will change state: {1} -> {2}".format(self.get_slot(), self.get_current_state(), target_state))
        self._nanny.set_instance_state(self._instance_url, target_state, self._logger)

    def up(self):
        self.set_target_state("ACTIVE")

    def down(self):
        self.set_target_state("PREPARED")

class NannySerivce:
    def __init__(self, nanny, service_url, logger):
        self._nanny = nanny
        self._service_url = service_url
        self._logger = logger

    def __repr__(self):
        return "<NannySerivce {0}>".format(self._service_url)

    def get_instances(self, regexp=None):
        return self._nanny.get_instances(self._service_url, regexp, self._logger)

class Nanny:
    def __init__(self, token):
        self._nanny_token = token
        self._nanny_url = "https://nanny.yandex-team.ru/v2/"

    def _get_session(self):
        session = requests.Session()
        session.headers['Authorization'] = 'OAuth {}'.format(self._nanny_token)
        session.headers['Content-Type'] = 'application/json;charset=UTF-8'
        return session

    def get_service(self, service, logger):
        logger.debug("Getting service {0}".format(service))
        url = urljoin(self._nanny_url, "services/{0}/current_state/".format(service))
        session = self._get_session()
        result = session.get(url, verify=VERIFY_SSL)

        if not result.ok:
            raise Exception("Could not get service {0}: {1}".format(service, result.text))

        result = json.loads(result.text)
        for snapshot in result["content"]["active_snapshots"]:
            if snapshot["state"] == "ACTIVE":
                return NannySerivce(self, "{0}/sn/{1}".format(service, snapshot["snapshot_id"]), logger)

        raise Exception("No active snapshots found")

    @retry
    def get_instances(self, service_url, regexp, logger):
        logger.debug("Getting instances of {0}".format(service_url))
        url = urljoin(self._nanny_url, "services/instances/{0}/".format(service_url))
        session = self._get_session()
        result = session.get(url, verify=VERIFY_SSL)

        if not result.ok:
            raise Exception("Could not get instances of service {0}: {1}".format(service_url, result.text))

        instances = []
        result = json.loads(result.text)
        for descr in result["instances"]:
            url = "{0}/engines/{1}/slots/{2}".format(service_url, descr["engine"], descr["slot"])
            if regexp and not re.search(regexp, descr["slot"]):
                continue
            instances.append(NannyInstance(self, url, descr, logger))

        return instances

    @retry
    def set_instance_state(self, instance_url, target_state, logger):
        url = urljoin(self._nanny_url, "services/instances/{0}/set_target_state/".format(instance_url))
        session = self._get_session()
        command = {"target": target_state, "comment": "Chaos-Nanny at work"}
        result = session.post(url, data=json.dumps(command), verify=VERIFY_SSL)

        if not result.ok:
            raise Exception("Could not change target state of instance {0}: {1}".format(instance_url, result.text))

        logger.info("Nanny reply: {0}".format(result.text))

class ServiceGroup:
    def __init__(self, nanny, name, description):
        self._nanny = nanny
        self._logger = logging.getLogger(name)
        self._services = [nanny.get_service(service, self._logger) for service in description["services"]]
        self._max_offline = description.get("offline", 1)
        self._sleep = description.get("sleep", 120)
        self._last_run_time = 0
        self._regexp = description.get("regexp", None)

    def get_instances(self):
        return sum([service.get_instances(self._regexp) for service in self._services], [])

    def cleanup(self):
        for instance in self.get_instances():
            if instance.get_target_state() != "ACTIVE":
                instance.up()

    def wait_for_instances(self):
        def _check():
            transitions = {}
            for instance in self.get_instances():
                currnet = instance.get_current_state()
                target = instance.get_target_state()
                if currnet not in ["PREPARED", "ACTIVE"]:
                    continue
                if currnet != target:
                    transitions[instance.get_slot()] = {
                        "current_state": currnet,
                        "target_state": target}
            if len(transitions) == 0:
                return True
            self._logger.info("Waiting for {0}".format(transitions))
            return False

        wait(_check)


    def chaos_step(self):
        if time.time() - self._last_run_time < self._sleep:
            return

        def _get_instances_per_state():
            result = {}
            for instance in self.get_instances():
                state = instance.get_current_state()
                result.setdefault(state, []).append(instance)
            return result

        self.wait_for_instances()
        per_state = _get_instances_per_state()

        online = len(per_state.get("ACTIVE", []))
        offline = len(per_state.get("PREPARED", []))
        activate = random.randint(0, max(0, self._max_offline - offline)) == 0
        self._logger.info("Current offline instances: {0}".format(offline))

        if activate and offline > 0:
            instance = random.choice(per_state["PREPARED"])
            instance.up()
        elif online > 0:
            instance = random.choice(per_state["ACTIVE"])
            instance.down()

        self._logger.info("Wait {0} seconds before next iteration".format(self._sleep))
        self._last_run_time = time.time()

def cleanup(serivce_groups):
    for serivce_group in serivce_groups:
        serivce_group.cleanup()

    for serivce_group in serivce_groups:
        serivce_group.wait_for_instances()

def chaos(serivce_groups):
    while True:
        for serivce_group in serivce_groups:
            serivce_group.chaos_step()
        time.sleep(1)

def chaos_and_cleanup(serivce_groups):
    try:
        chaos(serivce_groups)
    except KeyboardInterrupt:
        logging.info("Interrupted, quitting")
        cleanup(serivce_groups)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Randomly restart service instances.")
    parser.add_argument("--cleanup", action="store_true", default=False, help="Activate all instances (restore state from previous chaos runs)")
    parser.add_argument("--token", type=str, help="Nanny token path")
    parser.add_argument("services", type=str, help="Yson description of service groups {masters={services=[service1;service2;...];offline=1;sleep=120};nodes={...}}")
    args = parser.parse_args()

    token_path = os.path.expanduser(args.token if args.token else "~/.nanny/token")
    with open(token_path, "rb") as f:
        token = f.read().strip()

    nanny = Nanny(token)

    services = yson.loads(args.services)
    service_gropus = []
    for name, description in services.items():
        service_gropus.append(ServiceGroup(nanny, name, description))

    if args.cleanup:
        cleanup(service_gropus)
    else:
        chaos_and_cleanup(service_gropus)

