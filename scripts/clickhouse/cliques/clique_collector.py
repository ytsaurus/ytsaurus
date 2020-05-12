#!/usr/bin/python3
import json
import multiprocessing
from copy import deepcopy

import dictdiffer
from typing import Optional

import yt.wrapper as yt
import yt.yson as yson

import argparse
import prettytable
import time
import datetime
import dateutil.parser
import pytz

import logging

logger = logging.getLogger(__name__)

MST = pytz.timezone("Europe/Moscow")

def setup_logging(verbose):
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s\t%(levelname).1s\t%(module)s:%(lineno)d\t%(message)s")

    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(formatter)
    stderr_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    logging.root.addHandler(stderr_handler)


class Clique:
    ABORTED_JOB_REASONS = ["node_banned", "node_offline", "none", "other", "preemption", "resource_overdraft",
                           "scheduler", "revival_confirmation_timeout", "user_request"]
    JOB_STATES = ["total", "pending", "running", "completed", "failed", "aborted", "lost"]
    COMPONENTS = ["ytserver-clickhouse", "ytserver-log-tailer", "clickhouse-trampoline", "clickhouse", "launcher"]
    RESOURCES = ["memory", "cpu"]
    RESOURCES_TYPE = {"type_name": "struct", "members": [{"name": name, "type": "int64"} for name in RESOURCES]}
    MEMORY_CATEGORIES = ["uncompressed_block_cache", "clickhouse", "footprint"]

    SCHEMA = [
        {"name": "alias", "type_v3": "string"},
        {"name": "id", "type_v3": "string"},
        {"name": "title", "type_v3": {"type_name": "optional", "item": "string"}},
        {"name": "authenticated_user", "type_v3": "string"},
        {"name": "pools", "type_v3": {"type_name": "list", "item": "string"}},
        {"name": "start_time", "type_v3": {"type_name": "optional", "item": "datetime"}},
        {"name": "uptime", "type_v3": "interval"},
        {"name": "suspended", "type_v3": "bool"},
        {"name": "job_counter", "type_v3": {
            "type_name": "struct",
            "members": [{"name": name, "type": "int32"} for name in JOB_STATES]
        }},
        {"name": "aborted_jobs", "type_v3": {
            "type_name": "struct",
            "members": [{"name": name, "type": "int32"} for name in ABORTED_JOB_REASONS]
        }},
        {"name": "controller_agent_address", "type_v3": "string"},
        {"name": "previous_operation_id", "type_v3": {"type_name": "optional", "item": "string"}},
        {"name": "versions", "type_v3": {
            "type_name": "struct",
            "members": [{"name": name, "type": {"type_name": "optional", "item": "string"}} for name in COMPONENTS]
        }},
        {"name": "preemption_mode", "type_v3": "string"},
        {"name": "instance_resources", "type_v3": RESOURCES_TYPE},
        {"name": "instance_count", "type_v3": "int32"},
        {"name": "total_resources", "type_v3": RESOURCES_TYPE},
        {"name": "memory", "type_v3": {
            "type_name": "struct",
            "members": [{"name": name, "type": "int64"} for name in MEMORY_CATEGORIES],
        }},
        {"name": "started_by", "type_v3": {"type_name": "list", "item": "string"}},
        {"name": "config_file_path", "type_v3": "string"},
        {"name": "cluster_connection_diff", "type_v3": {"type_name": "list", "item": "string"}},
        {"name": "custom_config", "type_v3": {"type_name": "optional", "item": "yson"}},
    ]
    ATTRIBUTES = [column["name"] for column in SCHEMA]

    def __init__(self):
        self.brief_op = None
        self.op = None
        self.ytserver_clickhouse_config = None
        self.attributes = dict()

    def collect_attributes_from_brief_operation(self, brief_op: dict,
                                                observation_ts: Optional[datetime.datetime] = None):
        """
        :param brief_op: brief operation description from list_operations or full description from get_operation
        :param observation_ts: current moment of time; used to calculate uptime
        :return: None
        """
        self.attributes["id"] = brief_op["id"]
        self.attributes["authenticated_user"] = brief_op["authenticated_user"]
        self.attributes["pools"] = ["{}/{}".format(pool_tree, options.get("pool")) for pool_tree, options in
                                    brief_op["runtime_parameters"]["scheduling_options_per_pool_tree"].items()]
        self.attributes["alias"] = brief_op["brief_spec"]["alias"]
        self.attributes["suspended"] = brief_op["suspended"]
        self.attributes["title"] = brief_op["brief_spec"].get("title")
        self.attributes["start_time"] = dateutil.parser.parse(brief_op["start_time"]).astimezone(MST)
        self.attributes["uptime"] = \
            observation_ts - self.attributes["start_time"] if observation_ts is not None else None
        self.attributes["job_counter"] = brief_op["brief_progress"]["jobs"]

    def collect_attributes_from_operation(self, op: dict):
        """
        Collect more attributes from get_operation result.
        :param op: result of get_operation.
        :return: None
        """
        self.op = op
        self.attributes["aborted_jobs"] = {
            key: value for key, value in op["progress"]["jobs"]["aborted"]["scheduled"].items()
            if key in Clique.ABORTED_JOB_REASONS
        }
        self.attributes["controller_agent_address"] = op["controller_agent_address"]
        description = op["spec"].get("description", {})
        self.attributes["previous_operation_id"] = description.get("previous_operation_id")
        self.attributes["versions"] = {
            "ytserver-clickhouse": description.get("ytserver-clickhouse", {}).get("yt_version"),
            "ytserver-log-tailer": description.get("ytserver-log-tailer", {}).get("yt_version"),
            "clickhouse-trampoline": description.get("clickhouse-trampoline", {}).get("yt_version"),
            "clickhouse": description.get("ytserver-clickhouse", {}).get("ch_version"),
            "launcher": op["spec"].get("started_by", {}).get("wrapper_version")
        }
        task_spec = op["spec"]["tasks"].get("instances", {}) or op["spec"]["tasks"].get("clickhouse_servers", {})
        self.attributes["instance_resources"] = {
            "cpu": task_spec["cpu_limit"],
            "memory": task_spec["memory_limit"],
        }
        self.attributes["instance_count"] = task_spec["job_count"]
        self.attributes["total_resources"] = {
            "cpu": task_spec["cpu_limit"] * task_spec["job_count"],
            "memory": task_spec["memory_limit"] * task_spec["job_count"],
        }
        self.attributes["preemption_mode"] = op["full_spec"]["preemption_mode"]
        self.attributes["started_by"] = op["spec"]["started_by"]["command"]

        config_file_path = None
        for path in task_spec["file_paths"]:
            if path.attributes.get("file_name") == "config.yson":
                if config_file_path is not None:
                    raise ValueError("Duplicating config.yson in task spec")
                config_file_path = str(path)
        if config_file_path is None:
            raise ValueError("Missing config.yson in task spec")
        self.attributes["config_file_path"] = config_file_path

    def collect_attributes_from_ytserver_clickhouse_config(self, config: bytes, cluster_connection: dict):
        """
        Collect more attributes from ytserver-clickhouse config file.
        :param config: content of ytserver-clickhouse config file.
        :param cluster_connection: current content of cluster connection.
        :return: None
        """
        config = yson.loads(config)
        self.ytserver_clickhouse_config = config

        self.collect_cluster_connection(cluster_connection)

        def sanitize_config(config):
            config = deepcopy(config)
            config.pop("logging", None)
            config.pop("rpc_dispatcher", None)
            config.pop("cluster_connection", None)
            config.pop("user", None)
            config.pop("profile_manager", None)
            config.pop("addresses", None)
            config.pop("memory_watchdog", None)
            config.get("engine", {}).get("settings", {}).pop("max_threads", None)
            config.get("engine", {}).get("settings", {}).pop("max_distributed_connections", None)
            config.get("engine", {}).get("settings", {}).pop("max_memory_usage_for_all_queries", None)

            return config

        self.attributes["custom_config"] = sanitize_config(config)
        self.attributes["memory"] = {
            "uncompressed_block_cache": config["cluster_connection"].get("block_cache", {})
                .get("uncompressed_data", {}).get("capacity", 0),
            "clickhouse": config["engine"]["settings"]["max_memory_usage_for_all_queries"],
        }
        self.attributes["memory"]["footprint"] = self.attributes["instance_resources"]["memory"] - \
                                                 sum(self.attributes["memory"].values())

    def collect_cluster_connection(self, cluster_connection):
        def sanitize_cluster_connection(cluster_connection):
            cluster_connection = json.loads(json.dumps(cluster_connection))
            cluster_connection.pop("ignore_me_cc_max42", None)
            cluster_connection.pop("block_cache", None)
            return cluster_connection
        lhs = sanitize_cluster_connection(self.ytserver_clickhouse_config["cluster_connection"])
        rhs = sanitize_cluster_connection(cluster_connection)

        def compact_diff(diff):
            return ["/".join(map(str, entry[1])) for entry in diff]
        self.attributes["cluster_connection_diff"] = compact_diff(list(dictdiffer.diff(lhs, rhs, dot_notation=False)))

    def to_row(self, attribute_keys=None):
        attribute_keys = attribute_keys or Clique.ATTRIBUTES
        row = {key: self.attributes[key] for key in attribute_keys}
        replacements = {
            "start_time": lambda ts: int(ts.timestamp()),
            "uptime": lambda td: int(td / datetime.timedelta(microseconds=1))
        }
        for key, replacement in replacements.items():
            if key in row:
                row[key] = replacement(row[key])

        return row

    def __getitem__(self, key):
        return self.attributes[key]


def collect_config(clique):
    try:
        client = yt.YtClient(config=yt.config.get_config(client=None))
        content = client.read_file(clique.attributes["config_file_path"]).read()
        logger.debug("Length of config for clique %s is %d", clique.attributes["id"], len(content))
        return content
    except Exception:
        logger.exception("Error collecting config for clique %s", clique.attributes["id"])
        return None


def collect_configs(cliques):
    pool = multiprocessing.Pool(processes=100)
    result = pool.map(collect_config, cliques)
    pool.close()
    return result


def collect_cliques(observation_ts=None):
    logger.info("Listing operations")

    brief_ops = yt.list_operations(filter="is_clique", state="running")
    assert not brief_ops["incomplete"]

    logger.info("Collected %d running brief operations, building initial clique structures",
                len(brief_ops["operations"]))

    cliques = []
    for brief_op in brief_ops["operations"]:
        clique = Clique()
        try:
            clique.collect_attributes_from_brief_operation(brief_op, observation_ts=observation_ts)
            cliques.append(clique)
        except Exception:
            logger.exception("Malformed brief operation for clique %s", brief_op["id"])

    logger.info("Collected %d well-formed cliques basing on brief operations, performing batch get_operation",
                len(cliques))

    ops = yt.batch_apply(lambda clique, client=None: client.get_operation_attributes(clique.attributes["id"]), cliques)

    logger.info("Operations collected")

    old_cliques = cliques
    cliques = []
    for clique, op in zip(old_cliques, ops):
        try:
            clique.collect_attributes_from_operation(op)
            cliques.append(clique)
        except Exception:
            logger.exception("Malformed full operation for clique %s", op["id"])

    logger.info("Collected %d well-formed cliques basing on full operations, performing batch config collection",
                len(cliques))

    configs = collect_configs(cliques)

    logger.info("Configs collected")

    cluster_connection = yt.get("//sys/@cluster_connection")

    old_cliques = cliques
    cliques = []
    for clique, config in zip(old_cliques, configs):
        try:
            clique.collect_attributes_from_ytserver_clickhouse_config(config, cluster_connection=cluster_connection)
            cliques.append(clique)
        except Exception:
            logger.exception("Malformed ytserver-clickhouse config for clique %s", op["id"])

    logger.info("Collected %d cliques basing on full operations and ytserver-clickhouse configs", len(cliques))

    return cliques


def deduce_attributes(attributes):
    if attributes is not None:
        attributes = attributes
        for attribute in attributes:
            if attribute not in Clique.ATTRIBUTES:
                raise ValueError("Unknown attribute {}".format(attribute))
    else:
        attributes = Clique.ATTRIBUTES
    return attributes


def get_current_ts():
    return datetime.datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(MST)


def format_multiline(cell):
    if isinstance(cell, (dict, yson.YsonMap, list, yson.YsonList)):
        return yson.dumps(cell, yson_format="pretty").decode("ascii")
    return cell


def print_to_stdout(attributes=None, format=None):
    attributes = deduce_attributes(attributes)

    observation_ts = get_current_ts()

    logger.debug("Observation ts is %s", observation_ts)

    cliques = collect_cliques(observation_ts=observation_ts)
    if format == "tabular":
        table = prettytable.PrettyTable(field_names=attributes, hrules=prettytable.ALL)
        for attribute in attributes:
            table.align[attribute] = "l"
            table.valign[attribute] = "m"
        for clique in cliques:
            table.add_row([format_multiline(clique[attribute]) for attribute in attributes])
        print(table.get_string())
    elif format == "yson":
        print(yson.dumps([clique.attributes for clique in cliques], yson_format="pretty", yson_type="list_fragment")
              .decode("ascii"))
    else:
        raise ValueError("format should be one of yson|tabular")


def setup_table(path, include_observation_ts):
    expected_schema = Clique.SCHEMA
    if include_observation_ts:
        expected_schema = \
            [{"name": "observation_ts", "type_v3": "timestamp", "sort_order": "ascending"}] + expected_schema

    if yt.exists(path):
        logger.debug("Table {} already exists, schema:\n{}".format(
            path, yson.dumps(yt.get(path + "/@schema"), yson_format="pretty").decode("ascii")))
    else:
        logger.debug("Table {} does not exist".format(path))

    if path.append:
        yt.create("table", path, ignore_existing=True, attributes={"schema": expected_schema})
        yt.alter_table(path, schema=expected_schema)
    else:
        yt.create("table", path, force=True, attributes={"schema": expected_schema})


def write_table(path, attributes=None, include_observation_ts=False):
    attributes = deduce_attributes(attributes)

    path = yt.TablePath(path)

    setup_table(path, include_observation_ts)

    observation_ts = get_current_ts()

    rows = []
    for clique in collect_cliques(observation_ts=observation_ts):
        row = clique.to_row(attribute_keys=attributes)
        if include_observation_ts:
            row["observation_ts"] = int(1e6 * observation_ts.timestamp())
        rows.append(row)
    yt.write_table(path, rows)

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="COMMAND")
    subparsers.required = True

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Print lots of debugging information")

    print_subparser = subparsers.add_parser(
        "print", help="print cliques to stdout")
    print_subparser.add_argument(
        "--format", default="tabular", help="format; one of yson|tabular")
    print_subparser.add_argument(
        "--attributes", nargs="+", default=None, type=str,
        help="restrict to following list of attributes; defaults to all attributes")
    print_subparser.set_defaults(func=print_to_stdout)

    write_table_subparser = subparsers.add_parser(
        "write-table", help="write information about cliques to table")
    write_table_subparser.add_argument(
        "path", metavar="YPATH", type=str, help="table ypath to write rows with cliques")
    write_table_subparser.add_argument(
        "--include-observation-ts", action="store_true", help="put current timestamp as a key column")
    write_table_subparser.add_argument(
        "--attributes", nargs="+", default=None, type=str,
        help="restrict to following list of attributes; defaults to all attributes")
    write_table_subparser.set_defaults(func=write_table)

    args = parser.parse_args()
    setup_logging(args.verbose)

    kwargs = dict(vars(args))
    del kwargs["COMMAND"]
    del kwargs["func"]
    del kwargs["verbose"]
    args.func(**kwargs)


if __name__ == "__main__":
    main()
