from .conftest import authors
from .helpers import wait

from yt.wrapper.driver import get_api_version

import yt.wrapper as yt

import pytest

import copy
import random


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


_current_chaos_cell_tag = 100


def _generate_chaos_cell_tag():
    global _current_chaos_cell_tag
    _current_chaos_cell_tag += 1
    assert _current_chaos_cell_tag <= 10000
    return _current_chaos_cell_tag


def generate_chaos_cell_id():
    return _format_chaos_cell_id(_generate_chaos_cell_tag())


DEFAULT_SORTED_SCHEMA = [
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value", "type": "string"},
]

DEFAULT_REPLICATION_PROGRESS = {
    "segments": [{"lower_key": [], "timestamp": 1}],
    "upper_key": [yt.yson.to_yson_type(None, attributes={"type": "max"})],
}


@pytest.mark.usefixtures("yt_env_chaos")
class TestChaosCommands(object):
    def _sync_create_tablet_cell(self):
        cell_id = yt.create("tablet_cell", attributes={"size": 1})
        wait(lambda: yt.get("//sys/tablet_cells/{0}/@health".format(cell_id)) == "good")
        return cell_id

    def _sync_create_chaos_bundle_and_cell(self):
        # Only chaos with one cluster supported.

        clock_cluster_tag = yt.get("//sys/@primary_cell_tag")

        bundle_id = yt.create(
            type="chaos_cell_bundle",
            attributes={
                "name": "c",
                "chaos_options": {
                    "peers": [{}],
                },
                "options": {
                    "changelog_account": "sys",
                    "snapshot_account": "sys",
                    "peer_count": 1,
                    "independent_peers": False,
                    "clock_cluster_tag": clock_cluster_tag
                }
            }
        )

        wait(
            lambda: yt.exists("#{}".format(bundle_id))
            and yt.get("#{}/@life_stage".format(bundle_id)) == "creation_committed"
        )

        cell_id = generate_chaos_cell_id()

        assert yt.create(
            type="chaos_cell",
            attributes={
                "id": cell_id,
                "cell_bundle": "c",
                "area": "default",
            }
        ) == cell_id
        wait(lambda: yt.get("#{0}/@health".format(cell_id)) == "good")

        return cell_id

    def _create_chaos_table_replica(self, card_id, replica):
        attributes = {
            "enabled": replica.get("enabled", False),
            "replication_card_id": card_id,
        }
        for key in [
            "cluster_name", "replica_path", "content_type", "mode", "replication_card_id",
            "table_path", "catchup", "replication_progress", "enable_replicated_table_tracker"
        ]:
            if key in replica:
                attributes[key] = replica[key]

        return yt.create(
            type="chaos_table_replica",
            attributes=attributes,
        )

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("method", ["alter", "remove"])
    def test_replication_card_collocation_removed(self, method):
        # alter_replication_card has only v4 option.
        if get_api_version() == "v3" and method == "alter":
            pytest.skip()

        cell_id = self._sync_create_chaos_bundle_and_cell()
        yt.set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        def _create(prefix):
            crt = "{0}-crt".format(prefix)
            yt.create("chaos_replicated_table", crt, attributes={"chaos_cell_bundle": "c"})
            card_id = yt.get("{0}/@replication_card_id".format(crt))
            return crt, card_id

        crt1, card1 = _create("//tmp/a")
        crt2, card2 = _create("//tmp/b")

        collocation_id = yt.create("replication_card_collocation", None, attributes={
            "type": "replication",
            "table_paths": [crt1, crt2]
        })

        def _get_orchid_path(cell_id, path):
            address = yt.get("#{0}/@peers/0/address".format(cell_id))
            return "//sys/cluster_nodes/{0}/orchid/chaos_cells/{1}{2}".format(address, cell_id, path)
        collocation_path = _get_orchid_path(cell_id, "/chaos_manager/replication_card_collocations")
        assert len(yt.get("{0}/{1}/replication_card_ids".format(collocation_path, collocation_id))) == 2

        def _unbind(crt, card):
            if method == "remove":
                yt.remove(crt)
            else:
                yt.alter_replication_card(card, replication_card_collocation_id="0-0-0-0")

        _unbind(crt1, card1)
        wait(lambda: yt.get("{0}/{1}/replication_card_ids".format(collocation_path, collocation_id)) == [card2])
        _unbind(crt2, card2)
        wait(lambda: len(yt.get(collocation_path)) == 0)

    @authors("ponasenko-rs")
    def test_replication_progress_attribute(self):
        self._sync_create_tablet_cell()
        yt.set("//sys/accounts/tmp/@resource_limits/tablet_count", 1)

        cell_id = self._sync_create_chaos_bundle_and_cell()
        yt.set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        replica = {
            "cluster_name": "primary",
            "content_type": "data",
            "mode": "async",
            "enabled": True,
            "replica_path": "//tmp/t",
        }

        card_id = yt.create(
            type="replication_card",
            attributes={"chaos_cell_id": cell_id},
        )
        replica_id = self._create_chaos_table_replica(card_id, replica)

        yt.create(
            "table",
            replica["replica_path"],
            attributes={
                "upstream_replica_id": replica_id,
                "schema": DEFAULT_SORTED_SCHEMA,
                "dynamic": True,
            },
        )

        assert yt.get("//tmp/t/@replication_progress") == DEFAULT_REPLICATION_PROGRESS

        progress = copy.deepcopy(DEFAULT_REPLICATION_PROGRESS)
        progress.update({
            "segments": [{"lower_key": [], "timestamp": 2}]
        })
        yt.alter_table("//tmp/t", replication_progress=progress)

        assert yt.get("//tmp/t/@replication_progress") == progress
