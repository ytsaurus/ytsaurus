import json
import sys
from typing import ClassVar
from typing import IO

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .registry import scenario


@scenario
class DumpSpecScenario(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "dump_spec"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Dump tables spec only"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)
        self._out: IO | None = None

    def setup(self, **kwargs):
        self._out = kwargs.get("out", sys.stdout)

    def pre_action(self):
        assert self._out, "Nowhere to write"

        def _print_table(table: YtTable, cluster: YtCluster):
            table_spec = table.get_full_spec()
            from_spec = dict()
            from_spec["ordered"] = table.is_ordered
            if table.is_replicated:
                dict_repr = {
                    f"{replica_key[0]}:{replica_key[1]}": replica.yt_attributes
                    for replica_key, replica in table.replicas.items()
                }
                from_spec["replicas"] = dict_repr
                from_spec["in_collocation"] = bool(table.in_collocation)
                from_spec["is_rtt_enabled"] = bool(table.is_rtt_enabled)
                if table.is_rtt_enabled:
                    from_spec["preferred_sync_replicas"] = sorted(table.preferred_sync_replicas)
                else:
                    from_spec["sync_replicas"] = sorted(table.sync_replicas)
            if table.key in cluster.consumers:
                consumer = cluster.consumers[table.key]
                registration_list = []
                for registration_key in sorted(consumer.registrations):
                    registration = consumer.registrations[registration_key]
                    registration_list.append(
                        {
                            "cluster_name": registration.cluster_name,
                            "path": registration.path,
                            "vital": bool(registration.vital),
                            "partitions": registration.partitions if registration.partitions else [],
                        }
                    )
                from_spec["consumer"] = {"registrations": registration_list}
            full_spec = {"spec": from_spec, "yt_attributes": table_spec}

            print(f"[{table.table_type.upper()}] {table.cluster_name}:{table.path}", file=self._out)
            print(json.dumps(full_spec, sort_keys=True, indent=4), file=self._out)

        main_cluster = self.desired.main
        for table_key in sorted(main_cluster.tables):
            print(f"BEGIN_TABLE_FEDERATION {table_key}", file=self._out)
            main_table = main_cluster.tables[table_key]
            _print_table(main_table, main_cluster)
            for replica_cluster in self.desired.replicas:
                if table_key not in replica_cluster.tables:
                    continue
                replica_table = replica_cluster.tables[table_key]
                _print_table(replica_table, replica_cluster)
                if replica_table.chaos_replication_log:
                    log_table = replica_cluster.tables[replica_table.chaos_replication_log]
                    _print_table(log_table, replica_cluster)
            print(f"END_TABLE_FEDERATION {table_key}\n", file=self._out)

    def generate_actions(self) -> list[ActionBatch]:
        return []
