#!/usr/bin/python

import yt.wrapper as yt

import argparse
from collections import defaultdict

WELL_FORMED_KINDS = ("v0", "v1", "empty")

def fill_instances(clique, rsp):
    clique.alive_instances = []
    clique.dead_instances = []
    for instance in rsp:
        alive = False
        for lock in instance.attributes.get("locks", []):
            if lock["child_key"] == "lock":
                alive = True
        if alive:
            clique.alive_instances.append(str(instance))
        else:
            clique.dead_instances.append(str(instance))

def collect_v1_clique_intances(cliques):
    batch_client = yt.create_batch_client(raise_errors=True)
    rsps = []
    for clique in cliques:
        clique_id = str(clique)
        path = "//sys/clickhouse/cliques/" + clique_id
        rsps.append(batch_client.list(path, attributes=["locks", "id"]))
    batch_client.commit_batch()

    for clique, rsp in zip(cliques, rsps):
        fill_instances(clique, rsp.get_result())

def collect_operations():
    result = yt.list_operations(filter="is_clique", state="running")
    assert not result["incomplete"]
    return result["operations"]

def collect_cliques():
    cliques = yt.list("//sys/clickhouse/cliques", attributes=["count", "modification_time", "discovery_version"])
    by_kind = defaultdict(list)
    cypress_cliques = set()

    ops = collect_operations()
    op_id_to_op = dict()
    for op in ops:
        op_id_to_op[op["id"]] = op

    for clique in cliques:
        attributes = clique.attributes
        if attributes.get("count", 0) == 0:
            by_kind["empty"].append(clique)
        elif str(clique) not in op_id_to_op:
            by_kind["not_running_with_discovery"].append(clique)
        else:
            op = op_id_to_op[str(clique)]
            clique.attributes["pools"] = set(scheduling_options["pool"] for scheduling_options in op["runtime_parameters"]["scheduling_options_per_pool_tree"].itervalues())
            if attributes.get("discovery_version", 0) == 0:
                by_kind["v0"].append(clique)
            else:
                by_kind["v1"].append(clique)
        cypress_cliques.add(str(clique))

    collect_v1_clique_intances(by_kind["v1"])

    for op in ops:
        if op["brief_progress"]["jobs"]["running"] > 0 and op["id"] not in cypress_cliques:
            by_kind["running_without_discovery"].append(op["id"])

    return by_kind


def show_stats(args):
    cliques = collect_cliques()
    for kind, cliques in cliques.iteritems():
        print "kind =", kind
        for clique in cliques:
            if kind == "v1":
                print clique, clique.attributes, "dead count =", len(clique.dead_instances), "alive_count =", len(clique.alive_instances)
            else:
                print clique, clique.attributes


def collect_to_delete(kind, clique):
    if kind == "empty" or (kind == "v1" and len(clique.alive_instances) == 0):
        return ["//sys/clickhouse/cliques/" + str(clique)]
    elif kind == "v1":
        return ["//sys/clickhouse/cliques/" + str(clique) + "/" + str(instance) for instance in clique.dead_instances]
    else:
        return []

def collect_garbage(args):
    cliques = collect_cliques()
    if args.dry_run:
        for kind, cliques in cliques.iteritems():
            if kind not in WELL_FORMED_KINDS:
                continue
            for clique in cliques:
                to_delete = collect_to_delete(kind, clique)
                if len(to_delete) > 0:
                    print kind, clique, to_delete
    else:
        batch_client = yt.create_batch_client(raise_errors=True)
        rsps = []
        for kind, cliques in cliques.iteritems():
            if kind not in WELL_FORMED_KINDS:
                continue
            for clique in cliques:
                rsps += [(path, batch_client.remove(path, recursive=True)) for path in collect_to_delete(kind, clique)]
        batch_client.commit_batch()
        for path, rsp in rsps:
            print path, rsp.get_result() if rsp.is_ok() else rsp.get_error()


def suspend_operations(args):
    ops = collect_operations()
    to_suspend = [str(op["id"]) for op in ops if not op["suspended"]]
    if args.dry_run:
        for op in to_suspend:
            print op
    else:
        batch_client = yt.create_batch_client(raise_errors=True)
        rsps = []
        for op_id in to_suspend:
            rsps.append((op_id, batch_client.suspend_operation(op_id, abort_running_jobs=True)))
        batch_client.commit_batch()
        for op_id, rsp in rsps:
            print op_id, rsp.get_result() if rsp.is_ok() else rsp.get_error()


def resume_operations(args):
    ops = collect_operations()
    to_resume = [str(op["id"]) for op in ops if op["suspended"]]
    if args.dry_run:
        for op in to_resume:
            print op
    else:
        batch_client = yt.create_batch_client(raise_errors=True)
        rsps = []
        for op_id in to_resume:
            rsps.append((op_id, batch_client.resume_operation(str(op_id))))
        batch_client.commit_batch()
        for op_id, rsp in rsps:
            print op_id, rsp.get_result() if rsp.is_ok() else rsp.get_error()


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    stats_subparser = subparsers.add_parser("show-stats", help="print clique statistics")
    stats_subparser.set_defaults(func=show_stats)

    collect_garbage_subparser = subparsers.add_parser("collect-garbage", help="remove old nodes")
    collect_garbage_subparser.add_argument("--dry-run", action="store_true", help="only print actions to be done")
    collect_garbage_subparser.set_defaults(func=collect_garbage)

    control_operations_subparser = subparsers.add_parser("control-operations", help="suspend/resume operations")
    control_operations_subparser_subparsers = control_operations_subparser.add_subparsers()

    suspend_operations_subparser = control_operations_subparser_subparsers.add_parser("suspend", help="suspend operations")
    suspend_operations_subparser.add_argument("--dry-run", action="store_true", help="only print actions to be done")
    suspend_operations_subparser.set_defaults(func=suspend_operations)

    resume_operations_subparser = control_operations_subparser_subparsers.add_parser("resume", help="suspend operations")
    resume_operations_subparser.add_argument("--dry-run", action="store_true", help="only print actions to be done")
    resume_operations_subparser.set_defaults(func=resume_operations)



    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
