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
import os

###################################################################################################################
# Configuration Goes Here.

SERVICES = [
    # --- master ---
    {
        "type": "master",
        "solomon_id": "yt_bridge_master_internal",
        "solomon_name": "yt_master_internal",
        "yt_port": 10010,
        "solomon_port": 10020,
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
        "solomon_port": 10030,
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
        "solomon_port": 10040,
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
        "solomon_port": 10021,
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
        "solomon_port": 10031,
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
        "solomon_port": 10041,
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
        "solomon_port": 10022,
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
        "solomon_port": 10032,
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
        "solomon_port": 10042,
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
        "solomon_port": 10052,
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
        "solomon_port": 10023,
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
        "solomon_port": 10033,
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
        "solomon_port": 10043,
        "bridge_rules": [
            "+/rpc",
            "-.*",
        ],
    },
    # --- http proxy ---
    {
        "type": "http_proxy",
        "solomon_id": "yt_http_proxy",
        "solomon_name": "yt_http_proxy",
        "solomon_port": "10013",
    },
    # --- yp ---
    {
        "project": "yp",
        "type": "yp_master",
        "solomon_id": "yp_bridge_yp_master",
        "solomon_name": "yp_master",
        "yt_port": 9080,
        "solomon_port": 9020,
        "bridge_rules": [
            "+.*"
        ],
    },
]

CONDUCTOR_GROUPS = {
    "master": ["masters"],
    "scheduler": ["schedulers"],
    "node": ["nodes"],
    "rpc_proxy": ["nodes"],
    "http_proxy": ["proxy"],
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


PRESTABLE = "USE_PRESTABLE" in os.environ


@click.group()
def cli():
    pass


def get_solomon_api():
    return "http://solomon.yandex.net/api/v2" if not PRESTABLE else "http://solomon-prestable.yandex.net/api/v2"


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
    CLUSTERS_URL = "/projects/yt/clusters"

    def __init__(self, uri, id, token, local_data={}, remote_data={}):
        self.uri = uri
        self.id = id
        self.token = token
        self._local_data = local_data
        self._remote_data = remote_data

    @staticmethod
    def make_api_url(uri):
        print "make_api_url", get_solomon_api() + uri
        return get_solomon_api() + uri

    @staticmethod
    def make_admin_url(uri):
        return "https://solomon.yandex-team.ru/admin" + uri

    @property
    def api_url_modify(self):
        if len(self.id) == 0:
            return self.make_api_url(self.uri)
        return self.make_api_url(self.uri) + "/" + self.id

    @property
    def api_url_create(self):
        self._local_data["id"] = self.id
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

    def update(self, dry_run):
        if self.remote:
            self.save(dry_run)
        else:
            self.create()

    def try_update(self, dry_run):
        data = self._local_data
        loaded = self.try_load()
        if loaded:
            self._local_data.update(data)
            self.save(dry_run)
        else:
            self.create(dry_run)

    def create(self, dry_run):
        logging.debug("Creating resource '%s'%s", self.api_url_create, " (dry run)" if dry_run else "")
        if dry_run:
            view_object("Create object %s %s" % (self.api_url_create, self.id), self.local)
            return
        rsp = requests.post(self.api_url_create, headers=self.headers, json=self._local_data)
        if not rsp.ok:
            view_response(rsp)
        self._remote_data = rsp.json()
        self._local_data = copy.deepcopy(self._remote_data)

    def delete(self):
        logging.debug("Deleting resource '%s'", self.api_url_modify)
        rsp = requests.delete(self.api_url_modify, headers=self.headers)
        if not rsp.ok:
            view_response(rsp)

    def load(self):
        logging.debug("Loading resource '%s'", self.api_url_modify)
        rsp = requests.get(self.api_url_modify, headers=self.headers)
        if not rsp.ok:
            logging.debug("Response: %s", rsp.text)
            rsp.raise_for_status()
        self._remote_data = rsp.json()
        self._local_data = copy.deepcopy(self._remote_data)

    def try_load(self):
        logging.debug("Loading resource '%s'", self.api_url_modify)
        rsp = requests.get(self.api_url_modify, headers=self.headers)
        if not rsp.ok:
            if rsp.status_code == 404:
                return False
            logging.debug("Response: %s", rsp.text)
            rsp.raise_for_status()
        self._remote_data = rsp.json()
        self._local_data = copy.deepcopy(self._remote_data)
        return True

    def view_diff(self):
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

    def save(self, dry_run):
        if not self.is_dirty:
            logging.debug("Skipping saving resource '%s' because it is clean", self.uri)
            return

        logging.debug("Saving resource '%s'%s", self.uri, " (dry run)" if dry_run else "")
        if dry_run:
            self.view_diff()
        else:
            rsp = requests.put(self.api_url_modify, headers=self.headers, json=self._local_data)
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


@cli.command()
def bridge_conf():
    conf = {"sources": []}
    for service in SERVICES:
        if "solomon_port" not in service:
            continue
        conf["sources"].append(dict(
            solomon_id=service["solomon_id"],
            solomon_port=service["solomon_port"],
            url=("localhost:%s" % service["yt_port"]),
            rules=service["bridge_rules"]))
    json.dump(conf, sys.stdout, indent=2, sort_keys=True)


@cli.command()
@click.option("--token", required=True)
@click.option("--yes", is_flag=True)
def check_solomon_services(token, yes):
    for service in SERVICES:
        resource = Resource("/projects/%s/services" % service.get("project", "yt"), service["solomon_id"], token)
        resource.load()

        def f(key, value):
            resource.local[key] = value

        f("type", "JSON_GENERIC")

        f("port", service["solomon_port"])
        f("path", "/pbjson")
        f("interval", 15)
        f("addTsArgs", True)
        f("sensorConf", SENSOR)

        resource.save(dry_run=(not yes))


@cli.command()
@click.option("--token", required=True)
@click.option("--cluster", required=True)
@click.option("--type", multiple=True)
@click.option("--yes", is_flag=True)
def update_cluster_nodes(token, cluster, type, yes):
    solomon_cluster = normalize_cluster_id(cluster)
    types = get_cluster_types() if not type else type

    for type in types:
        conductor_groups = get_conductor_groups(solomon_cluster, type)
        solomon_cluster_type = get_solomon_cluster_type(solomon_cluster, type)
        solomon_conductor_groups = [{
            "group": group,
            "labels": ["type=%s" % type]
        } for group in conductor_groups]

        cluster_services = Resource(Resource.CLUSTERS_URL, solomon_cluster_type, token, local_data={
            "name": cluster,
            "projectId": "yt",
            "conductorGroups": solomon_conductor_groups,
        })
        cluster_services.try_update(dry_run=(not yes))


@cli.command()
@click.option("--token", required=True)
@click.option("--cluster", required=True)
@click.option("--type", multiple=True)
@click.option("--yes", is_flag=True)
def update_cluster_services(token, cluster, type, yes):
    solomon_cluster = normalize_cluster_id(cluster)
    types = get_cluster_types() if not type else type
    type_set = set(types)

    cluster_services = {}
    for type in types:
        solomon_cluster_type = get_solomon_cluster_type(solomon_cluster, type)
        services_resource = Resource("/projects/yt/clusters/%s" % solomon_cluster_type, "services", token)
        services_resource.load()
        for service in services_resource.remote:
            cluster_services[service["id"]] = service

    tag = "".join(random.choice("0123456789abcdef") for _ in range(8))
    for service_description in SERVICES:
        type = service_description["type"]
        if type not in type_set:
            continue
        solomon_cluster_type = get_solomon_cluster_type(solomon_cluster, type)
        service = service_description["solomon_id"]
        if service in cluster_services:
            logging.info("Service %s is already configured for cluster %s, skipping", service, cluster)
            continue
        shard_name = "_".join([solomon_cluster, service_description["solomon_name"].replace("yt_", ""), tag])
        data = {
            "projectId": service_description.get("project", "yt"),
            "serviceId": service,
            "clusterId": solomon_cluster_type,
            #"quotas": {
            #    "maxSensorsPerUrl": 80000,
            #    "maxResponseSizeMb": 10,
            #    "maxFileSensors": 100000,
            #    "maxMemSensors": 100000,
            #},
        }
        shard = Resource("/projects/yt/shards", shard_name, token, local_data=data)
        shard.try_update(dry_run=(not yes))


@cli.command()
@click.option("--token", required=True)
@click.option("--cluster", required=True)
@click.option("--type", multiple=True)  # type can be empty to cleanup old scheme
@click.option("--yes", is_flag=True)
def unlink_cluster(token, cluster, type, yes):
    solomon_cluster = normalize_cluster_id(cluster)
    cluster_services = get_cluster_services()
    types = get_cluster_types() if not type else type

    for type in types:
        solomon_cluster_type = get_solomon_cluster_type(solomon_cluster, type) if type else solomon_cluster
        resource = Resource("/projects/yt/clusters/%s" % solomon_cluster_type, "services", token)
        resource.load()

        for item in resource.local:
            if item["id"] not in cluster_services:
                continue
            shard_id = item["shardId"]
            if yes:
                Resource("/projects/yt/shards", shard_id, token).delete()
            else:
                view_object("Deleting shard '%s'" % shard_id, item)


@cli.command()
@click.option("--token", required=True)
@click.option("--mode", type=click.Choice(["pretty", "table"]), default="pretty")
def list_shards(token, mode):
    resource = Resource("/projects/yt/shards?pageSize=all", "", token)
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
    import yt.wrapper as yt

    assert cluster in CLUSTER_NODE_MASKS
    yt.config["proxy"]["url"] = cluster
    masks = CLUSTER_NODE_MASKS[cluster]
    cluster_solomon = "yt_%s_tablet_nodes" % cluster
    resource = Resource("/projects/yt/clusters", cluster_solomon, token)
    resource.load()

    logging.info("Getting tablet nodes from cluster %s", cluster)
    nodes = yt.get("//sys/nodes", attributes=["tablet_slots", "io_weights"])
    tablet_nodes = []
    for node, attributes in nodes.items():
        slots = attributes.attributes.get("tablet_slots", [])
        if len(slots) > 0:
            tablet_nodes.append(node)
        else:
            io_weights = attributes.attributes.get("io_weights", {})
            for media, weight in io_weights.items():
                if media.startswith("ssd_journals") and weight > 0:
                    tablet_nodes.append(node)
                    break
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
