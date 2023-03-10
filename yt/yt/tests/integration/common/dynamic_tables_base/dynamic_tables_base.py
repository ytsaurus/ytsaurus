from yt_env_setup import YTEnvSetup

from yt_helpers import profiler_factory

from yt_commands import (wait, ls, get, create_dynamic_table, set_node_decommissioned, disable_tablet_cells_on_node)

from yt.common import YtError

import yt.yson as yson


class DynamicTablesBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 0
    USE_DYNAMIC_TABLES = True

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout": 2000,
            "peer_revocation_timeout": 3000,
        }
    }

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }

    def _create_sorted_table(self, path, **attributes):
        if "schema" not in attributes:
            schema = yson.YsonList([
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
            schema.attributes["unique_keys"] = True
            attributes.update({"schema": schema})
        create_dynamic_table(path, **attributes)

    def _create_ordered_table(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update(
                {
                    "schema": [
                        {"name": "key", "type": "int64"},
                        {"name": "value", "type": "string"},
                    ]
                }
            )
        create_dynamic_table(path, **attributes)

    def _get_recursive(self, path, result=None, driver=None):
        if result is None or result.attributes.get("opaque", False):
            result = get(path, attributes=["opaque"], driver=driver)
        if isinstance(result, dict):
            for key, value in list(result.items()):
                result[key] = self._get_recursive(path + "/" + key, value, driver=driver)
        if isinstance(result, list):
            for index, value in enumerate(result):
                result[index] = self._get_recursive(path + "/" + str(index), value, driver=driver)
        return result

    def _find_tablet_orchid(self, address, tablet_id, driver=None):
        def _do():
            path = "//sys/cluster_nodes/{}/orchid/tablet_cells".format(address)
            cells = ls(path, driver=driver)
            for cell_id in cells:
                if get("{}/{}/hydra/active".format(path, cell_id), False, driver=driver):
                    tablets = ls("{}/{}/tablets".format(path, cell_id), driver=driver)
                    if tablet_id in tablets:
                        try:
                            return self._get_recursive("{}/{}/tablets/{}".format(path, cell_id, tablet_id), driver=driver)
                        except YtError:
                            return None
            return None

        for attempt in range(5):
            result = _do()
            if result is not None:
                return result
        return None

    def _get_pivot_keys(self, path):
        tablets = get(path + "/@tablets")
        return [tablet["pivot_key"] for tablet in tablets]

    def _decommission_all_peers(self, cell_id):
        addresses = []
        peers = get("#" + cell_id + "/@peers")
        for x in peers:
            addr = x["address"]
            addresses.append(addr)
            set_node_decommissioned(addr, True)
        return addresses

    def _get_table_profiling(self, table, user=None):
        class Profiling:
            def __init__(self):
                self.profiler = profiler_factory().at_tablet_node(table)
                self.tags = {
                    "table_path": table,
                }
                if user is not None:
                    self.tags["user"] = user

            def get_counter(self, counter_name, tags={}):
                return self.profiler.get(
                    counter_name,
                    dict(self.tags, **tags),
                    postprocessor=float,
                    verbose=False,
                    default=0)

            def get_all_time_max(self, counter_name, tags={}):
                return self.profiler.get(
                    counter_name,
                    dict(self.tags, **tags),
                    postprocessor=lambda data: data.get('all_time_max'),
                    summary_as_max_for_all_time=True,
                    export_summary_as_max=True,
                    verbose=False,
                    default=0)

            def has_projections_with_tags(self, counter_name, required_tags):
                return len(self.profiler.get_all(counter_name, required_tags, verbose=False)) > 0

        return Profiling()

    def _disable_tablet_cells_on_peer(self, cell):
        peer = get("#{0}/@peers/0/address".format(cell))
        disable_tablet_cells_on_node(peer, "disable tablet cells on peer")

        def check():
            peers = get("#{0}/@peers".format(cell))
            if len(peers) == 0:
                return False
            if "address" not in peers[0]:
                return False
            if peers[0]["address"] == peer:
                return False
            return True

        wait(check)
