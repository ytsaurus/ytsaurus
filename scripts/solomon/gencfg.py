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
import collections

import yt.wrapper as yt

###################################################################################################################
# Configuration Goes Here.

SERVICES = [
    # --- master ---
    {
        "type": "master",
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
        "type": "master",
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
        "type": "master",
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
        "type": "scheduler",
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
        "type": "scheduler",
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
        "type": "scheduler",
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
        "type": "node",
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
        "type": "node",
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
        "type": "node",
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
        "type": "node",
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
        "type": "rpc_proxy",
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
        "type": "rpc_proxy",
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
        "type": "rpc_proxy",
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

CONDUCTOR_GROUPS = {
    "master": ["masters"],
    "scheduler": ["schedulers"],
    "node": ["nodes"],
    "rpc_proxy": ["nodes"],
}

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

CLUSTER_NODE_MASKS = {
    "hahn": ["n%s-sas.hahn.yt.yandex.net"],
    "banach": ["p%si.banach.yt.yandex.net", "n%si.banach.yt.yandex.net"],
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
    def make_cluster_uri(cluster=None):
        if cluster:
            return Resource.make_api_url("/projects/yt/clusters/%s" % cluster)
        else:
            return Resource.make_api_url("/projects/yt/clusters")

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

    @staticmethod
    def create(uri, data, token):
        logging.debug("Creating resource '%s'", uri)
        rsp = requests.post(uri, headers={"Authorization": "OAuth " + token}, json=data)
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
        if not rsp.ok:
            rsp.raise_for_status()
        self._remote_data = rsp.json()
        self._local_data = copy.deepcopy(self._remote_data)

    def try_load(self):
        logging.debug("Loading resource '%s'", self.uri)
        rsp = requests.get(self.api_url, headers=self.headers)
        if not rsp.ok:
            if rsp.status_code == 404:
                return False
            rsp.raise_for_status()
        self._remote_data = rsp.json()
        self._local_data = copy.deepcopy(self._remote_data)
        return True

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
            if "version" in self.remote:
                message += "## [version=%s]\n" % self.remote["version"]
            if "deleted" in self.remote:
                message += "## [deleted=%s]\n" % self.remote["deleted"]
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


class NodeRanges(object):
    def __init__(self):
        self.numbers = []

    def append(self, number):
        self.numbers.append(number)

    @property
    def ranges(self):
        return " ".join(["{0}-{0}".format(number) for number in sorted(self.numbers)])


def normalize_cluster_id(cluster):
    cluster = cluster.replace("-", "_")
    if not cluster.startswith("yt_"):
        cluster = "yt_" + cluster
    return cluster


def extract_cluster_names(name):
    return set(map(lambda s: s[name], SERVICES))


def get_cluster_types():
    return extract_cluster_names("type")


def get_cluster_services():
    return extract_cluster_names("solomon_id")


def get_solomon_cluster_type(cluster, type):
    return "%s_%s" % (cluster, type)


def get_conductor_groups(cluster, type):
    return [get_solomon_cluster_type(cluster, group) for group in CONDUCTOR_GROUPS[type]]


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
def update_solomon_cluster(token, cluster, yes):
    solomon_cluster = normalize_cluster_id(cluster)
    types = get_cluster_types()

    for type in types:
        conductor_groups = get_conductor_groups(solomon_cluster, type)
        solomon_cluster_type = get_solomon_cluster_type(solomon_cluster, type)
        solomon_conductor_groups = [{
            "group": group,
            "labels": ["type=%s" % type]
        } for group in conductor_groups]

        cluster_services = Resource(
            Resource.make_cluster_uri(solomon_cluster_type),
            token,
            local_data={
                "id": solomon_cluster_type,
                "name": cluster,
                "projectId": "yt",
                "conductorGroups": solomon_conductor_groups,
            })
        loaded = cluster_services.try_load()
        if loaded:
            cluster_services.local["conductorGroups"] = solomon_conductor_groups
            cluster_services.save(dry_run=(not yes))
        else:
            logging.info("Creating cluster type %s" % type)
            Resource.create(Resource.make_cluster_uri(), data={
                "id": solomon_cluster_type,
                "name": cluster,
                "projectId": "yt",
                "conductorGroups": solomon_conductor_groups,
            }, token=token)


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
            Resource.create("/projects/yt/shards", data, token)
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
    assert cluster in CLUSTER_NODE_MASKS
    yt.config["proxy"]["url"] = cluster
    masks = CLUSTER_NODE_MASKS[cluster]
    cluster_solomon = "yt_%s_tablet_nodes" % cluster
    resource = Resource("/projects/yt/clusters/%s" % cluster_solomon, token)
    resource.load()

    logging.info("Getting tablet nodes from cluster %s", cluster)
    nodes = yt.get("//sys/nodes", attributes=["tablet_slots"])
    tablet_nodes = []
    for node, attributes in nodes.items():
        slots = attributes.attributes.get("tablet_slots", [])
        if len(slots) > 0:
            tablet_nodes.append(node)
    logging.info("Found %s tablet nodes", len(tablet_nodes))

    logging.info("Generating table nodes ranges")
    ranges = collections.defaultdict(NodeRanges)
    for node in tablet_nodes:
        digits = re.search(r"(\d+)", node).group(0)
        assert len(digits) == 4
        appended = False
        for i, mask in enumerate(masks):
            if node.startswith(mask % digits):
                ranges[i].append(digits)
                appended = True
        if not appended:
            raise Exception("Cannot find mask for %s" % node)

    def update(ranges, mask):
        pattern = mask % "%04d"
        hosts = resource.local["hosts"]
        for host in hosts:
            if host["urlPattern"] == pattern:
                host["ranges"] = ranges
                return
        hosts.append({
            "urlPattern": pattern,
            "ranges": ranges,
            "labels": [
                "type=tablet_node"
            ]
        })

    logging.info("Updating solomon")
    for i, mask in enumerate(masks):
        update(ranges[i].ranges, mask)
    resource.save(dry_run=(not yes))


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG)
    cli()
