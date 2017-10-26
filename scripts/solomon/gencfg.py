#!/usr/bin/env python

import copy
import click
import requests
import json
import sys
import pprint
import logging
import random
import re

import yt.wrapper as yt

###################################################################################################################
# Configuration Goes Here.

SERVICES = [
    # --- master ---
    {
        "solomon_id": "yt_bridge_master_internal",
        "solomon_name": "yt_master_internal",
        "yt_port": 10010,
        "bridge_port": 10020,
        "bridge_rules": [
            "+/(action_queue|bus|lf_alloc|logging|monitoring|profiling|resource_tracker)",
            "-.*",
        ],
    },
    {
        "solomon_id": "yt_bridge_master_other",
        "solomon_name": "yt_master",
        "yt_port": 10010,
        "bridge_port": 10030,
        "bridge_rules": [
            "-/(action_queue|bus|lf_alloc|logging|monitoring|profiling|resource_tracker|rpc)",
            "+.*",
        ],
    },
    {
        "solomon_id": "yt_bridge_master_rpc",
        "solomon_name": "yt_master_rpc",
        "yt_port": 10010,
        "bridge_port": 10040,
        "bridge_rules": [
            "+/rpc",
            "-.*",
        ],
    },
    # --- scheduler ---
    {
        "solomon_id": "yt_bridge_scheduler_internal",
        "solomon_name": "yt_scheduler_internal",
        "yt_port": 10011,
        "bridge_port": 10021,
        "bridge_rules": [
            "+/(action_queue|bus|lf_alloc|logging|monitoring|profiling|resource_tracker)",
            "-.*",
        ],
    },
    {
        "solomon_id": "yt_bridge_scheduler_other",
        "solomon_name": "yt_scheduler",
        "yt_port": 10011,
        "bridge_port": 10031,
        "bridge_rules": [
            "-/(action_queue|bus|lf_alloc|logging|monitoring|profiling|resource_tracker|rpc)",
            "+.*",
        ],
    },
    {
        "solomon_id": "yt_bridge_scheduler_rpc",
        "solomon_name": "yt_scheduler_rpc",
        "yt_port": 10011,
        "bridge_port": 10041,
        "bridge_rules": [
            "+/rpc",
            "-.*",
        ],
    },
    # --- node ---
    {
        "solomon_id": "yt_bridge_node_internal",
        "solomon_name": "yt_node_internal",
        "yt_port": 10012,
        "bridge_port": 10022,
        "bridge_rules": [
            "+/(action_queue|bus|lf_alloc|logging|monitoring|profiling|resource_tracker)",
            "-.*",
        ],
    },
    {
        "solomon_id": "yt_bridge_node_other",
        "solomon_name": "yt_node",
        "yt_port": 10012,
        "bridge_port": 10032,
        "bridge_rules": [
            "-/(action_queue|bus|lf_alloc|logging|monitoring|profiling|resource_tracker|rpc)",
            "-/tablet_node/(write|commit|select|lookup|replica)/",
            "+.*",
        ],
    },
    {
        "solomon_id": "yt_bridge_node_rpc",
        "solomon_name": "yt_node_rpc",
        "yt_port": 10012,
        "bridge_port": 10042,
        "bridge_rules": [
            "+/rpc",
            "-.*",
        ],
    },
    {
        "solomon_id": "yt_bridge_node_tablet_profiling",
        "solomon_name": "yt_node_tablet_profiling",
        "yt_port": 10012,
        "bridge_port": 10052,
        "bridge_rules": [
            "+/tablet_node/(write|commit|select|lookup|replica)/",
            "-.*",
        ],
    },
    # --- rpc proxy ---
    {
        "solomon_id": "yt_bridge_rpc_proxy_internal",
        "solomon_name": "yt_rpc_proxy_internal",
        "yt_port": 10014,
        "bridge_port": 10023,
        "bridge_rules": [
            "+/(action_queue|bus|lf_alloc|logging|monitoring|profiling|resource_tracker)",
            "-.*",
        ],
    },
    {
        "solomon_id": "yt_bridge_rpc_proxy_other",
        "solomon_name": "yt_rpc_proxy",
        "yt_port": 10014,
        "bridge_port": 10033,
        "bridge_rules": [
            "-/(action_queue|bus|lf_alloc|logging|monitoring|profiling|resource_tracker|rpc)",
            "+.*",
        ],
    },
    {
        "solomon_id": "yt_bridge_rpc_proxy_rpc",
        "solomon_name": "yt_rpc_proxy_rpc",
        "yt_port": 10014,
        "bridge_port": 10043,
        "bridge_rules": [
            "+/rpc",
            "-.*",
        ],
    },
]

SENSOR = {
    "aggrRules": [
      {"cond": ["host=*"], "target": ["host=Aggr"]},
      {"cond": ["DC=*"], "target": ["host=Aggr_DC_{{DC}}"]},
    ],
    "priorityRules": [
      {"target": "host=Aggr_DC_Ugr", "priority": 10},
      {"target": "host=Aggr_DC_Sas", "priority": 10},
      {"target": "host=Aggr_DC_Fol", "priority": 10},
      {"target": "host=Aggr_DC_Iva", "priority": 10},
      {"target": "host=Aggr_DC_Myt", "priority": 10},
      {"target": "host=Aggr_DC_Ash", "priority": 10},
      {"target": "host=Aggr_DC_Ams", "priority": 10},
      {"target": "host=Aggr_DC_Veg", "priority": 10},
      {"target": "host=Aggr_DC_Man", "priority": 10},
      {"target": "host=Aggr", "priority": 100}
    ],
    "rawDataMemOnly": False,
}

CLUSTER_NODES_MASKS = {
    "hahn": "n%s-sas.hahn.yt.yandex.net",
    "banach": "n%si.banach.yt.yandex.net",
}

###################################################################################################################
# Code Goes Here.


def view_object(title, obj, color="blue"):
    message = ""
    message += click.style("## %s" % title, fg=color) + "\n"
    message += click.style("#" * 80, fg=color) + "\n"
    message += "\n"
    message += pprint.pformat(obj)
    click.echo_via_pager(message)


def view_response(rsp):
    if rsp.status_code >= 400:
        color = "red"
    elif rsp.status_code >= 200 and rsp.status_code < 300:
        color = "green"
    else:
        color = "blue"
    message = ""
    message += click.style("## %s   %s" % (rsp.status_code, rsp.url), fg=color) + "\n"
    message += click.style("#" * 80, fg=color) + "\n"
    message += "\n"
    message += pprint.pformat(rsp.json())
    click.echo_via_pager(message)

class Resource(object):
    def __init__(self, uri, token, local_data={}, remote_data={}):
        self.uri = uri
        self.token = token
        self._local_data = local_data
        self._remote_data = remote_data

    @staticmethod
    def make_api_url(uri):
        return "http://solomon.yandex.net/api/v2" + uri

    @staticmethod
    def make_admin_url(uri):
        return "https://solomon.yandex-team.ru/admin" + uri

    @property
    def api_url(self):
        return self.make_api_url(self.uri)

    @property
    def admin_url(self):
        return self.make_admin_url(self.uri)

    @property
    def local(self):
        return self._local_data

    @property
    def remote(self):
        return self._remote_data

    @property
    def is_dirty(self):
        for key in self._local_data:
            local = self._local_data.get(key, None)
            remote = self._remote_data.get(key, None)
            if local != remote:
                return True
        return False

    @property
    def headers(self):
        return {"Authorization": "OAuth " + self.token}

    def create(self, uri, data):
        logging.debug("Creating resource '%s'", uri)
        rsp = requests.post(self.api_url, headers=self.headers, json=data)
        if not rsp.ok:
            view_response(rsp)

    def delete(self, uri):
        logging.debug("Deleting resource '%s'", uri)
        rsp = requests.delete(self.api_url, headers=self.headers)
        if not rsp.ok:
            view_response(rsp)

    def load(self):
        logging.debug("Loading resource '%s'", self.uri)
        rsp = requests.get(self.api_url, headers=self.headers)
        assert rsp.ok
        self._remote_data = rsp.json()
        self._local_data = copy.deepcopy(self._remote_data)

    def save(self, dry_run):
        if not self.is_dirty:
            logging.debug("Skipping saving resource '%s' because it is clean", self.uri)
            return

        logging.debug("Saving resource '%s'%s", self.uri, " (dry run)" if dry_run else "")
        if dry_run:
            message = ""
            message += "#" * 80 + "\n"
            message += "## Dry run for resource '%s'\n" % self.uri
            message += "## %s\n" % self.admin_url
            if "deleted" in self.remote:
                message += "## [version=%s; deleted=%s]\n" % (self.remote["version"], self.remote["deleted"])
            message += "#" * 80 + "\n"
            message += "\n"
            for key in self._local_data:
                local = self._local_data.get(key, None)
                remote = self._remote_data.get(key, None)
                if local == remote:
                    continue
                message += "#" * 80 + "\n"
                message += "## Key '%s'\n" % key
                message += "# --- (before) ---\n"
                message += pprint.pformat(remote, indent=2) + "\n\n"
                message += "# --- (after) ---\n"
                message += pprint.pformat(local, indent=2) + "\n\n"
                message += "\n\n"
            click.echo_via_pager(message)
        else:
            rsp = requests.put(self.api_url, headers=self.headers, json=self._local_data)
            if rsp.ok:
                self._remote_data = rsp.json()
                self._local_data = copy.deepcopy(self._remote_data)


def normalize_cluster_id(cluster):
    cluster = cluster.replace("-", "_")
    if not cluster.startswith("yt_"):
        cluster = "yt_" + cluster
    return cluster


@click.group()
def cli():
    pass


@cli.command()
def bridge_conf():
    conf = {"sources": []}
    for service in SERVICES:
        conf["sources"].append(dict(
            solomon_id=service["solomon_id"],
            solomon_port=service["bridge_port"],
            url=("localhost:%s" % service["yt_port"]),
            rules=service["bridge_rules"]))
    json.dump(conf, sys.stdout, indent=2, sort_keys=True)


@cli.command()
@click.option("--token", required=True)
@click.option("--yes", is_flag=True)
def check_solomon_services(token, yes):
    for service in SERVICES:
        resource = Resource("/projects/yt/services/%s" % service["solomon_id"], token)
        resource.load()

        def f(key, value):
            resource.local[key] = value

        f("type", "JSON_GENERIC")
        f("port", service["bridge_port"])
        f("path", "/pbjson")
        f("interval", 15)
        f("addTsArgs", True)
        f("sensorConf", SENSOR)

        resource.save(dry_run=(not yes))


@cli.command()
@click.option("--token", required=True)
@click.option("--cluster", required=True)
@click.option("--yes", is_flag=True)
def link_cluster(token, cluster, yes):
    cluster = normalize_cluster_id(cluster)

    good_services = set(map(lambda s: s["solomon_id"], SERVICES))

    resource = Resource("/projects/yt/clusters/%s/services" % cluster, token)
    resource.load()

    conflicting = False
    for item in resource.local:
        if item["id"] in good_services:
            logging.error("Service '%s' in already linked!", item["id"])
            conflicting = True
    if conflicting:
        return

    tag = "".join(random.choice("0123456789abcdef") for _ in range(8))
    for service in good_services:
        data = {
            "id": "_".join(["yt", cluster.replace("yt_", ""), service.replace("yt_", ""), tag]),
            "projectId": "yt",
            "serviceId": service,
            "clusterId": cluster,
            #"quotas": {
            #    "maxSensorsPerUrl": 80000,
            #    "maxResponseSizeMb": 10,
            #    "maxFileSensors": 100000,
            #    "maxMemSensors": 100000,
            #},
        }
        if yes:
            Resource.create("/projects/yt/shards", data)
        else:
            view_object("Creating shard", data)


@cli.command()
@click.option("--token", required=True)
@click.option("--cluster", required=True)
@click.option("--yes", is_flag=True)
def unlink_cluster(token, cluster, yes):
    cluster = normalize_cluster_id(cluster)

    good_services = set(map(lambda s: s["solomon_id"], SERVICES))

    resource = Resource("/projects/yt/clusters/%s/services" % cluster, token)
    resource.load()

    for item in resource.local:
        if item["id"] not in good_services:
            continue
        uri = "/projects/yt/shards/%s" % item["shardId"]
        if yes:
            Resource.delete(uri)
        else:
            view_object("Deleting shard '%s'" % uri, item)


@cli.command()
@click.option("--token", required=True)
@click.option("--mode", type=click.Choice(["pretty", "table"]), default="pretty")
def list_shards(token, mode):
    resource = Resource("/projects/yt/shards?pageSize=65535", token)
    resource.load()

    if mode == "pretty":
        view_object("Shards", resource.local)
    elif mode == "table":
        for item in resource.local["result"]:
            print "\t".join([item["id"], item["clusterId"], item["serviceId"]])


@cli.command()
@click.option("--token", required=True)
@click.option("--cluster", required=True)
@click.option("--yes", is_flag=True)
def update_tablet_nodes(token, cluster, yes):
    assert cluster in CLUSTER_NODES_MASKS
    yt.config["proxy"]["url"] = cluster
    mask = CLUSTER_NODES_MASKS[cluster]
    cluster_solomon = "yt_%s_tablet_nodes" % cluster
    resource = Resource("/projects/yt/clusters/%s" % cluster_solomon, token)
    resource.load()
    nodes = yt.get("//sys/nodes", attributes=["tablet_slots"])
    tablet_nodes = []
    for node, attributes in nodes.items():
        slots = attributes.attributes.get("tablet_slots", [])
        if len(slots) > 0:
            tablet_nodes.append(node)
    logging.info("Found %s tablet nodes", len(tablet_nodes))
    node_numbers = []
    for node in tablet_nodes:
        digs = re.search(r"(\d+)", node).group(0)
        assert len(digs) == 4
        node_numbers.append(digs)
    node_numbers = sorted(node_numbers)
    pattern_range = ""
    for number in node_numbers:
        pattern_range += " %s-%s" % (number, number)
    pattern_range = pattern_range[1:]
    resource.local["hosts"][0]["ranges"] = pattern_range
    resource.save(dry_run=(not yes))


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG)
    cli()
