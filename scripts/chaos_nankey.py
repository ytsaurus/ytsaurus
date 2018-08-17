#!/usr/bin/python

import requests
import argparse
import json
from urlparse import urljoin

import sys
import os
import random

import logging
import time

#logging.basicConfig(level=logging.DEBUG)

def wait(predicate):
    while not predicate():
        time.sleep(2)

class NannyInstance:
    def __init__(self, nanny, instance_url, descr):
        self._nanny = nanny
        self._instance_url = instance_url
        self._descr = descr

    def __repr__(self):
        return "<NannyInstance {0}>".format(self._instance_url)

    def get_slot(self):
        return self._descr["slot"]

    def get_target_state(self):
        return self._descr["target_state"]
    def get_current_state(self):
        return self._descr["current_state"]

    def set_target_state(self, target_state):
        print "Instance {0} will change state: {1} -> {2}".format(self.get_slot(), self.get_current_state(), target_state)
        self._nanny.set_instance_state(self._instance_url, target_state)

    def up(self):
        self.set_target_state("ACTIVE")

    def down(self):
        self.set_target_state("PREPARED")



class NannySerivce:
    def __init__(self, nanny, service_url):
        self._nanny = nanny
        self._service_url = service_url

    def __repr__(self):
        return "<NannySerivce {0}>".format(self._service_url)

    def get_instances(self):
        return self._nanny.get_instances(self._service_url)

    def wait_for_instances(self):
        def _check():
            transitions = {}
            instances = self.get_instances()
            for instance in instances:
                currnet = instance.get_current_state()
                target = instance.get_target_state()
                if currnet != target:
                    transitions[instance.get_slot()] = {
                        "current_state": currnet,
                        "target_state": target}
            if len(transitions) == 0:
                return True
            print transitions
            return False

        wait(_check)

class Nanny:
    def __init__(self, token):
        self._nanny_token = token
        self._nanny_url = "https://nanny.yandex-team.ru/v2/"

    def _get_session(self):
        session = requests.Session()
        session.headers['Authorization'] = 'OAuth {}'.format(self._nanny_token)
        session.headers['Content-Type'] = 'application/json;charset=UTF-8'
        return session

    def get_service(self, service):
        url = urljoin(self._nanny_url, "services/{0}/current_state/".format(service))
        session = self._get_session()
        result = session.get(url)

        if not result.ok:
            raise Exception("Could not get service {0}: {1}".format(service, result.text))

        result = json.loads(result.text)
        for snapshot in result["content"]["active_snapshots"]:
            if snapshot["state"] == "ACTIVE":
                return NannySerivce(self, "{0}/sn/{1}".format(service, snapshot["snapshot_id"]))

        raise Exception("No active snapshots found")

    def get_instances(self, service_url):
        url = urljoin(self._nanny_url, "services/instances/{0}/".format(service_url))
        session = self._get_session()
        result = session.get(url)

        if not result.ok:
            raise Exception("Could not get instances of service {0}: {1}".format(service_url, result.text))

        instances = []
        result = json.loads(result.text)
        for descr in result["instances"]:
            url = "{0}/engines/{1}/slots/{2}".format(service_url, descr["engine"], descr["slot"])
            instances.append(NannyInstance(self, url, descr))

        return instances

    def set_instance_state(self, instance_url, target_state):
        url = urljoin(self._nanny_url, "services/instances/{0}/set_target_state/".format(instance_url))
        session = self._get_session()
        command = {"target": target_state, "comment": "Chaos-Nanny at work"}
        result = session.post(url, data=json.dumps(command))

        if not result.ok:
            raise Exception("Could not change target state of instance {0}: {1}".format(instance_url, result.text))

        print result.text


def cleanup(serivces):
    for service in services:
        instances = service.get_instances()
        for instance in instances:
            if instance.get_target_state() != "ACTIVE":
                instance.up()

    for service in services:
        service.wait_for_instances()

def chaos(services, max_offline):
    def _wait():
        for service in services:
            service.wait_for_instances()
    def _get_instances():
        instances = []
        for service in services:
            instances += service.get_instances()
        return instances

    def _split_statistics(instances):
        result = {}
        for instance in instances:
            state = instance.get_current_state()
            result.setdefault(state, []).append(instance)
        return result

    while True:
        _wait()
        instances = _get_instances()
        per_state = _split_statistics(instances)

        online = len(per_state.get("ACTIVE", []))
        offline = len(per_state.get("PREPARED", []))
        activate = random.uniform(0, max(0, max_offline - offline)) == 0

        if activate and offline > 0:
            instance = random.choice(per_state["PREPARED"])
            instance.up()
        elif online > 0:
            instance = random.choice(per_state["ACTIVE"])
            instance.down()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Randomly restart nodes and possibly schedulers.")
    parser.add_argument("--cleanup", action="store_true", default=False, help="Restart nodes")
    parser.add_argument("--token", type=str, help="Nanny token path")
    parser.add_argument("services", type=str, nargs="+", help="Nanny services")
    parser.add_argument("--offline", type=int, default=1, help="Number of instances which can be offline")
    args = parser.parse_args()

    token_path = os.path.expanduser(args.token if args.token else "~/.nanny/token")
    with open(token_path, "rb") as f:
        token = f.read().strip()

    nanny = Nanny(token)

    services = []
    for service_name in args.services:
        service = nanny.get_service(service_name)
        services.append(service)

    if args.cleanup:
        cleanup(services)
    else:
        chaos(services, args.offline)

