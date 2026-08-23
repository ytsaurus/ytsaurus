import random
from time import sleep

from .create import create_sorted_table, make_schema
from .log import logger


BUNDLE_NAME = "chaos_bundle"


def _format_uuid_part(value):
    return hex(value)[2:].rstrip("L")


def _generate_uuid_part(limit=2 ** 32):
    return _format_uuid_part(random.randint(0, limit - 1))


def _format_chaos_cell_id(cell_tag):
    # See IsWellKnownId.
    # EObjectType::ChaosCell == 1200
    return _generate_uuid_part(2 ** 16) + "-" + \
        _generate_uuid_part() + "-" + \
        _format_uuid_part(2 ** 16 * cell_tag + 1200) + "-" + \
        _generate_uuid_part()


def _setup_chaos_bundle(client):
    if client.exists(f"//sys/chaos_cell_bundles/{BUNDLE_NAME}"):
        return
    client.create("chaos_cell_bundle", attributes={
        "name": BUNDLE_NAME,
        "chaos_options": {
            "peers": [{}],
        },
        "options": {
            "changelog_account": "sys",
            "snapshot_account": "sys",
            "snapshot_replication_factor": 1,
            "changelog_replication_factor": 1,
            "changelog_write_quorum": 1,
            "changelog_read_quorum": 1,
        },
    })


def _setup_chaos_cell(client):
    existing = client.list("//sys/chaos_cells")
    if existing:
        return existing[0]
    cell_id = _format_chaos_cell_id(5000)
    client.create("chaos_cell", attributes={"id": cell_id, "cell_bundle": BUNDLE_NAME})
    while client.get(f"#{cell_id}/@health") != "good":
        logger.info("Waiting for chaos cell")
        sleep(0.5)
    return cell_id


def create_chaos_table_with_replicas(client, major_name, table_path):
    _setup_chaos_bundle(client)
    cell_id = _setup_chaos_cell(client)
    client.set(f"//sys/chaos_cell_bundles/{BUNDLE_NAME}/@metadata_cell_ids", [cell_id])

    card_id = client.create("replication_card", attributes={"chaos_cell_id": cell_id})
    logger.info(f"Created replication card {card_id} for {table_path}")

    client.create("chaos_replicated_table", table_path, force=True, attributes={
        "chaos_cell_bundle": BUNDLE_NAME,
        "replication_card_id": card_id,
        "schema": make_schema(with_hash=True),
    })

    for mode in ["sync", "async"]:
        queue_path = f"{table_path}_queue_{mode}"
        replica_id = client.create("chaos_table_replica", force=True, attributes={
            "replication_card_id": card_id,
            "cluster_name": major_name,
            "replica_path": queue_path,
            "content_type": "queue",
            "mode": mode,
            "enabled": True,
        })
        client.create("replication_log_table", queue_path, force=True, attributes={
            "dynamic": True,
            "schema": make_schema(with_hash=True),
            "upstream_replica_id": replica_id,
            "chaos_cell_bundle": BUNDLE_NAME,
        })
        client.reshard_table(queue_path, tablet_count=10, uniform=True)
        client.mount_table(queue_path, sync=True)

    for mode in ["sync", "async"]:
        data_path = f"{table_path}_{mode}"
        replica_id = client.create("chaos_table_replica", force=True, attributes={
            "replication_card_id": card_id,
            "cluster_name": major_name,
            "replica_path": data_path,
            "content_type": "data",
            "mode": mode,
            "enabled": True,
        })
        create_sorted_table(
            data_path,
            attributes={
                "upstream_replica_id": replica_id,
                "chaos_cell_bundle": BUNDLE_NAME,
            },
            schema_attributes={"with_hash": True},
            force=True,
            ignore_existing=True,
            client=client)
        client.reshard_table(data_path, tablet_count=10, uniform=True)
        client.mount_table(data_path, sync=True)

    _wait_for_sync_replica_no_errors(client, table_path)
    _wait_for_replication_era(client, card_id, table_path)


def _wait_for_sync_replica_no_errors(client, table_path):
    sync_data_path = table_path + "_sync"
    tablet_ids = [t["tablet_id"] for t in client.get(sync_data_path + "/@tablets")]

    def _all_tablets_no_errors(tablet_ids=tablet_ids):
        for tablet_id in tablet_ids:
            try:
                orchid = client.get("#{0}/orchid".format(tablet_id))
                errors = orchid.get("errors", [])
                if errors:
                    logger.info(
                        "Waiting for tablet %s of %s to clear errors: %s",
                        tablet_id, sync_data_path,
                        [e.get("inner_errors", [{}])[0].get("message") for e in errors],
                    )
                    return False
            except Exception:
                return False
        return True

    logger.info("Waiting for sync replica tablets of %s to have no errors", table_path)
    while not _all_tablets_no_errors():
        sleep(0.5)
    logger.info("Sync replica tablets of %s have no errors", table_path)


def _wait_for_replication_era(client, card_id, table_path):
    card = client.get(f"#{card_id}/@")
    era = card.get("era")
    logger.info("Waiting for replication era %s to propagate to data replica tablets", era)

    for mode in ["sync", "async"]:
        data_path = f"{table_path}_{mode}"
        tablets = client.get(data_path + "/@tablets")
        tablet_ids = [t["tablet_id"] for t in tablets]

        def _all_tablets_at_era(tablet_ids=tablet_ids, data_path=data_path):
            for tablet_id in tablet_ids:
                try:
                    orchid = client.get("#{0}/orchid".format(tablet_id))
                    rep_card = orchid.get("replication_card")
                    if rep_card is None or rep_card.get("era") != era:
                        logger.info(
                            "Waiting for tablet %s of %s: era=%s, want=%s",
                            tablet_id, data_path,
                            rep_card.get("era") if rep_card else None,
                            era,
                        )
                        return False
                except Exception:
                    return False
            return True

        while not _all_tablets_at_era():
            sleep(0.5)

    logger.info("Replication era %s propagated to all data replica tablets of %s", era, table_path)
