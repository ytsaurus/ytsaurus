from .logger import logger
from .table_creation import create_dynamic_table
from .job_base import JobBase
from .schema import Schema
from .table_creation import create_table_if_not_exists
from .stateless_write import RandomWriteMapper
from .helpers import create_client
from .verify import verify_tables_equal

from yt.wrapper.http_helpers import get_proxy_url

import yt.wrapper as yt

import copy
import sys
import time
import random

################################################################################

class Replica(object):
    def __init__(self, desc, schema):
        assert desc["cluster"] is None, "Remote replicas are not supported"

        self.path = desc["path"]
        self.cluster = desc["cluster"] or get_proxy_url().split(".")[0]
        self.mode = desc["mode"]
        self.enable = desc["enable"]
        self.schema = schema

################################################################################

def create_replica_table(replica, attributes):
    attributes = copy.deepcopy(attributes)
    attributes["upstream_replica_id"] = replica.replica_id
    def do_create():
        create_dynamic_table(replica.path, replica.schema, attributes)
    def do_check(path):
        upstream_replica_id = yt.get(path + "/@upstream_replica_id")
        return upstream_replica_id == replica.replica_id
    create_table_if_not_exists(replica.path, replica.schema, True, do_create, do_check)


def create_replica(table_path, replica):
    for id, existing in yt.get(table_path + "/@replicas").items():
        if existing["cluster_name"] == replica.cluster and \
                existing["replica_path"] == replica.path:
            replica.replica_id = id
            if existing["mode"] != replica.mode:
                yt.alter_table_replica(replica.replica_id, mode=replica.mode)
            logger.info("Replica %s@%s already exists and has id %s, using it",
                replica.path, replica.cluster, replica.replica_id)
            return
    id = yt.create("table_replica", attributes={
        "table_path": table_path,
        "cluster_name": replica.cluster,
        "replica_path": replica.path,
        "mode": replica.mode,
    })
    replica.replica_id = id


def initialize_replicas(table_path, schema, attributes, spec):
    replicas = []
    for replica_spec in spec.replicas:
        replicas.append(Replica(replica_spec, copy.deepcopy(schema)))

    # Set initial replication modes.
    for replica in replicas:
        if replica.mode is None:
            replica.mode = "async"

    # Create replica objects.
    for replica in replicas:
        create_replica(table_path, replica)

    # Create pivots.
    for replica in replicas:
        replica.schema.create_pivot_keys(spec.size.tablet_count)

    # Create replica tables.
    for replica in replicas:
        create_replica_table(replica, attributes)

    # Enable replicas.
    for replica in replicas:
        logger.info("Enabling replicas")
        yt.alter_table_replica(replica.replica_id, enabled=replica.enable)

    return replicas


def create_replicated_table(table_path, schema, attributes, spec):
    def do_create():
        create_dynamic_table(
            table_path, schema, attributes, object_type="replicated_table")
    def do_check(path):
        return True
    schema.create_pivot_keys(spec.size.tablet_count)
    create_table_if_not_exists(table_path, schema, True, do_create, do_check)


def check_replicas_equal(table_path, lhs, rhs):
    ts = yt.generate_timestamp()
    tablet_count = yt.get(table_path + "/@tablet_count")
    client_v4 = create_client(api_version="v4")

    while True:
        try:
            tablet_infos = client_v4.get_tablet_infos(
                path=table_path, tablet_indexes=list(range(tablet_count)))
        except Exception as e:
            print(e)
            time.sleep(1)
            continue

        caught_up = True
        for tablet in tablet_infos["tablets"]:
            for id in (lhs.replica_id, rhs.replica_id):
                (replica, ) = [
                    r for r in tablet["replica_infos"]
                    if r["replica_id"] == id]
                if replica["last_replication_timestamp"] < ts:
                    caught_up = False
                    logger.info("Replica %s has not caught up (NeededTs: %x, GotTs: %x)",
                        id, ts, replica["last_replication_timestamp"])
                    break

        if caught_up:
            break
        time.sleep(1)

    result = yt.create_temp_table()
    logger.info("Validating replicas %s and %s, ts=%s, result=%s",
        lhs.path, rhs.path, ts, result)

    verify_tables_equal(
        lhs.path, rhs.path, result, rhs.schema.get_column_names(),
        rhs.schema.get_key_column_names(), should_sort=False,
        timestamp=ts)


def set_random_replication_modes(replicas, spec):
    while True:
        modes = [random.choice(("sync", "async")) for i in replicas]
        min = spec.replicated.min_sync_replicas
        max = spec.replicated.max_sync_replicas
        if max is None:
            max = len(replicas)
        if min <= modes.count("sync") <= max:
            break
    for replica, mode in zip(replicas, modes):
        if replica.mode != mode:
            replica.mode = mode
            logger.info("Switching replica %s to %s mode",
                replica.path, mode)
            yt.alter_table_replica(replica.replica_id, mode=mode)


def run_compare_replicas(table_path, spec, attributes, args):
    schema = Schema(sorted=True, spec=spec)
    create_replicated_table(table_path, schema, attributes, spec)
    replicas = initialize_replicas(table_path, schema, attributes, spec)

    RandomWriteMapper.BATCH_SIZE = 100

    with yt.TempTable() as fake_input:
        yt.write_table(fake_input, [{"a": "b"} for i in range(1000)])

        op_spec = {
            "title": "Eternal stateless writer",
            "job_count": 1000,
            "scheduling_options_per_pool_tree": {
                "physical": {
                    "resource_limits": {
                        "user_slots": 5}}}}
        with yt.TempTable() as fake_output:
            try:
                op = yt.run_map(
                    RandomWriteMapper(schema, table_path, args),
                    fake_input,
                    fake_output,
                    spec=op_spec,
                    sync=False)
                logger.info("Operation started. Use the following command to update "
                    "running job count:")
                print("yt update-op-parameters {} '{{scheduling_options_per_pool_tree="
                    "{{physical={{resource_limits={{user_slots={}}}}}}}}}'".format(
                        op.id, 100), file=sys.stderr)

                idx = 0
                while True:
                    r1 = replicas[idx]
                    idx = (idx + 1) % len(replicas)
                    r2 = replicas[idx]
                    check_replicas_equal(table_path, r1, r2)

                    if spec.replicated.switch_replica_modes:
                        set_random_replication_modes(replicas, spec)
            except (Exception, KeyboardInterrupt) as e:
                print(e)
                op.abort()
                logger.info("Waiting for transactions to be aborted")
                time.sleep(5)
                raise
