from yt_env_setup import YTEnvSetup

from yt_helpers import profiler_factory

from yt_commands import (
    wait, ls, get, set, exists, create_dynamic_table, set_node_decommissioned,
    disable_tablet_cells_on_node, get_driver, get_cluster_drivers, print_debug
)

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

    class CellsDisabled():
        def __init__(self, clusters, tablet_bundles=[], chaos_bundles=[], area_ids=[]):
            self._clusters = clusters
            self._tablet_bundles = tablet_bundles
            self._chaos_bundles = chaos_bundles
            self._area_ids = area_ids

        def __enter__(self):
            cell_orchids = self._capture_cell_orchids()
            self._set_tag_filters("invalid")
            self._wait_for_cells_failed(cell_orchids)

        def __exit__(self, exc_type, exception, traceback):
            self._set_tag_filters("")
            self._wait_for_cells_good()

        def _set_tag_filters(self, tag_filter):
            for cluster in self._clusters:
                driver = get_driver(cluster=cluster)
                for bundle in self._tablet_bundles:
                    set("//sys/tablet_cell_bundles/{0}/@node_tag_filter".format(bundle), tag_filter, driver=driver)
                for bundle in self._chaos_bundles:
                    set("//sys/chaos_cell_bundles/{0}/@node_tag_filter".format(bundle), tag_filter, driver=driver)
                for area_id in self._area_ids:
                    set("#{0}/@node_tag_filter".format(area_id), tag_filter, driver=driver)

        def _peer_orchid(self, node_address, cell_id):
            cell_type = int(cell_id.split("-")[2], 16) & 0xffff
            return "//sys/cluster_nodes/{0}/orchid/{1}_cells/{2}".format(
                node_address,
                {700: "tablet", 1200: "chaos"}[cell_type],
                cell_id
            )

        def _capture_cell_orchids(self, strict=False):
            cells_by_cluster = {}
            for cluster in self._clusters:
                driver = get_driver(cluster=cluster)
                cell_ids = []

                for bundle in self._tablet_bundles:
                    cell_ids += get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle), driver=driver)
                for bundle in self._chaos_bundles:
                    cell_ids += get("//sys/chaos_cell_bundles/{0}/@tablet_cell_ids".format(bundle), driver=driver)
                for area_id in self._area_ids:
                    cell_ids += get("#{0}/@cell_ids".format(area_id), driver=driver)

                cells = [
                    cell for cell in
                    ls("//sys/tablet_cells", attributes=["id", "peers"], driver=driver)
                    + ls("//sys/chaos_cells", attributes=["id", "peers"], driver=driver)
                    if cell.attributes["id"] in cell_ids
                ]
                assert len(cells) == len(cell_ids)

                if strict and any(["address" not in peer for cell in cells for peer in cell.attributes["peers"]]):
                    return None

                cells_by_cluster[cluster] = {
                    cell.attributes["id"]: [
                        self._peer_orchid(peer["address"], cell.attributes["id"])
                        for peer in cell.attributes["peers"]
                        if not peer.get("alien", False)
                    ]
                    for cell in cells
                }

            return cells_by_cluster

        def _wait_for_cells_good(self):
            print_debug("Waiting for cells to become good after reenabling...")

            def _check_orchids():
                cell_orchids = self._capture_cell_orchids(strict=True)
                if cell_orchids is None:
                    return False

                for cluster in self._clusters:
                    driver = get_driver(cluster=cluster)
                    for cell_id, peers_orchids in cell_orchids[cluster].items():
                        expected_config_version = get("#{0}/@config_version".format(cell_id), driver=driver)

                        try:
                            config_versions = [get("{0}/config_version".format(orchid), driver=driver) for orchid in peers_orchids]
                            if any(version != expected_config_version for version in config_versions):
                                print_debug("Cell {0} is not ready: expected config version: {1}, versions got: {2}".format(
                                    cell_id,
                                    expected_config_version,
                                    config_versions
                                ))
                                return False

                            peers_active = [get("{0}/hydra/active".format(orchid), driver=driver) for orchid in peers_orchids]
                            if not all(peers_active):
                                print_debug("Cell {0} is not ready: some peers are not active: {1}".format(cell_id, peers_active))
                                return False

                            peers_healths = [get("#{0}/@health".format(cell_id), driver=other_driver) for other_driver in get_cluster_drivers(driver)]
                            if not all(health == "good" for health in peers_healths):
                                print_debug("Cell {0} is not ready: some peers are not healthy: {1}".format(cell_id, peers_healths))
                                return False
                        except YtError:
                            return False

                return True

            wait(_check_orchids)

        def _wait_for_cells_failed(self, cell_orchids):
            print_debug("Waiting for cells to fail after disabling...")
            for cluster in self._clusters:
                driver = get_driver(cluster=cluster)
                for cell_id, peers_orchids in cell_orchids[cluster].items():
                    for orchid in peers_orchids:
                        wait(lambda: not exists(orchid, driver=driver))

                    for other_driver in get_cluster_drivers(driver):
                        wait(lambda: get("#{0}/@health".format(cell_id), driver=other_driver) in ["failed", "degraded"])

                    peers = get("#{0}/@peers".format(cell_id), driver=driver)
                    assert all(peer.get("alien", False) or peer["state"] == "none" for peer in peers)

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

    def _get_store_chunk_ids(self, path):
        for _ in range(5):
            try:
                chunk_ids = get(path + "/@chunk_ids")
                return [chunk_id for chunk_id in chunk_ids if get("#{}/@chunk_type".format(chunk_id)) == "table"]
            except YtError as err:
                if not err.is_resolve_error():
                    raise
        raise RuntimeError("Method get_store_chunk_ids failed")

    def _get_hunk_chunk_ids(self, path):
        for _ in range(5):
            try:
                chunk_ids = get(path + "/@chunk_ids")
                return [chunk_id for chunk_id in chunk_ids if get("#{}/@chunk_type".format(chunk_id)) == "hunk"]
            except YtError as err:
                if not err.is_resolve_error():
                    raise
        raise RuntimeError("Method get_hunk_chunk_ids failed")

    def _get_delta_profiling_wrapper(self, profiling_path, counters, table, user=None):
        self_ = self

        class Wrapper:
            def __init__(self):
                self.tablet_profiling = self_._get_table_profiling(table, user)
                self.profiling_path = profiling_path
                self.amount = len(counters)
                self.counters = counters
                self.values = [self._get_counter(counters[i]) for i in range(self.amount)]
                self.deltas = [0] * self.amount

            def _get_counter(self, counter):
                return self.tablet_profiling.get_counter(f"{self.profiling_path}/{counter}")

            def get_deltas(self):
                self.deltas = [self._get_counter(self.counters[i]) - self.values[i] for i in range(self.amount)]
                return self.deltas

            def commit(self):
                for i in range(self.amount):
                    self.values[i] += self.deltas[i]
                    self.deltas[i] = 0

        return Wrapper()

    def _get_key_filter_profiling_wrapper(self, method, table, user=None):
        entry = "range" if method == "select" else "key"

        def get_counter(counter_pref):
            return f"{counter_pref}_{entry}_count"

        return self._get_delta_profiling_wrapper(
            f"{method}/{entry}_filter",
            [get_counter("input"), get_counter("filtered_out"), get_counter("false_positive")],
            table,
            user,
        )
