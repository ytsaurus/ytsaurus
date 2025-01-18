from copy import deepcopy
import uuid

import pytest

from yt.yson.yson_types import YsonEntity
from yt.yt_sync.core.constants import CONSUMER_ATTRS
from yt.yt_sync.core.constants import CONSUMER_SCHEMA
from yt.yt_sync.core.model.cluster import YtCluster
from yt.yt_sync.core.model.helpers import get_node_name
from yt.yt_sync.core.model.helpers import make_log_name
from yt.yt_sync.core.model.node import YtNode
from yt.yt_sync.core.model.replica import YtReplica
from yt.yt_sync.core.model.schema import YtSchema
from yt.yt_sync.core.model.table import QueueOptions
from yt.yt_sync.core.model.table import RttOptions
from yt.yt_sync.core.model.table import YtTable
from yt.yt_sync.core.model.types import Types
from yt.yt_sync.core.spec.details import SchemaSpec

from .helpers import make_replica
from .helpers import YtReplicaTestBase


class TestRttOptions:
    @pytest.mark.parametrize("explicit_rtt", [True, False, None])
    def test_make_explicit_rtt(self, explicit_rtt: bool | None):
        rtt_options = RttOptions.make(
            {"replicated_table_tracker_enabled": True, "max_sync_replica_count": 2}, explicit_rtt_enabled=explicit_rtt
        )
        assert rtt_options.enabled == (explicit_rtt is None or explicit_rtt)
        assert rtt_options.preferred_sync_replica_clusters is None
        assert rtt_options.min_sync_replica_count is None
        assert rtt_options.max_sync_replica_count == 2
        assert rtt_options.attributes == {}

    def test_make_full(self):
        rtt_options = RttOptions.make(
            {
                "replicated_table_tracker_enabled": True,
                "preferred_sync_replica_clusters": ["remote0"],
                "min_sync_replica_count": 1,
                "sync_replica_count": 2,
                "enable_preload_check": True,
            }
        )
        assert rtt_options.enabled
        assert rtt_options.preferred_sync_replica_clusters == {"remote0"}
        assert rtt_options.min_sync_replica_count == 1
        assert rtt_options.max_sync_replica_count == 2
        assert rtt_options.attributes == {"enable_preload_check": True}

    def test_clear(self):
        rtt_options = RttOptions.make(
            {
                "replicated_table_tracker_enabled": True,
                "preferred_sync_replica_clusters": ["remote0"],
                "min_sync_replica_count": 1,
                "sync_replica_count": 2,
                "enable_preload_check": True,
            }
        )
        rtt_options.clear()
        assert not rtt_options.enabled
        assert rtt_options.preferred_sync_replica_clusters is None
        assert rtt_options.min_sync_replica_count is None
        assert rtt_options.max_sync_replica_count is None
        assert rtt_options.attributes == {}

    def test_not_has_diff_with(self):
        assert not RttOptions.make(
            {"replicated_table_tracker_enabled": True, "preferred_sync_replica_clusters": None}
        ).has_diff_with(
            RttOptions.make({"enable_replicated_table_tracker": True, "preferred_sync_replica_clusters": []})
        )

        assert not RttOptions.make(
            {
                "replicated_table_tracker_enabled": True,
                "preferred_sync_replica_clusters": ["remote0"],
                "min_sync_replica_count": 1,
                "sync_replica_count": 2,
                "enable_preload_check": True,
            }
        ).has_diff_with(
            RttOptions.make(
                {
                    "enable_replicated_table_tracker": True,
                    "preferred_sync_replica_clusters": ["remote0"],
                    "min_sync_replica_count": 1,
                    "max_sync_replica_count": 2,
                    "enable_preload_check": True,
                }
            )
        )

        assert not RttOptions.make(
            {
                "replicated_table_tracker_enabled": True,
                "preferred_sync_replica_clusters": ["remote0", "remote1"],
            }
        ).has_diff_with(
            RttOptions.make(
                {
                    "replicated_table_tracker_enabled": True,
                    "preferred_sync_replica_clusters": ["remote1", "remote0"],
                }
            )
        )

    def test_has_diff_with(self):
        assert RttOptions.make({"replicated_table_tracker_enabled": True}).has_diff_with(
            RttOptions.make({"replicated_table_tracker_enabled": False})
        )

        assert RttOptions.make({"preferred_sync_replica_clusters": ["remote0"]}).has_diff_with(
            RttOptions.make({"preferred_sync_replica_clusters": ["remote1"]})
        )

        assert RttOptions.make(
            {
                "replicated_table_tracker_enabled": True,
                "min_sync_replica_count": 1,
            }
        ).has_diff_with(
            RttOptions.make(
                {
                    "replicated_table_tracker_enabled": True,
                    "min_sync_replica_count": None,
                }
            )
        )

        assert RttOptions.make(
            {
                "replicated_table_tracker_enabled": True,
                "max_sync_replica_count": 1,
            }
        ).has_diff_with(
            RttOptions.make(
                {
                    "replicated_table_tracker_enabled": True,
                    "max_sync_replica_count": None,
                }
            )
        )

        assert RttOptions.make({"min_sync_replica_count": 1}).has_diff_with(
            RttOptions.make({"min_sync_replica_count": 2})
        )

        assert RttOptions.make({"max_sync_replica_count": 1}).has_diff_with(
            RttOptions.make({"max_sync_replica_count": 2})
        )

        assert RttOptions.make({"enable_preload_check": True}).has_diff_with(
            RttOptions.make({"enable_preload_check": False})
        )

    def test_changed_attributes(self):
        desired = RttOptions.make({"replicated_table_tracker_enabled": True})
        actual = RttOptions.make({"replicated_table_tracker_enabled": False})

        actual_changes: dict[str, tuple[bool, bool]] = dict()
        for path, desired, actual in desired.changed_attributes(actual):
            actual_changes[path] = (desired, actual)

        assert actual_changes == {"replicated_table_options/enable_replicated_table_tracker": (True, False)}

    def test_yt_attributes(self):
        assert RttOptions.make(
            {
                "replicated_table_tracker_enabled": True,
                "preferred_sync_replica_clusters": ["remote1", "remote0"],
                "min_sync_replica_count": 1,
                "sync_replica_count": 2,
                "enable_preload_check": True,
            }
        ).yt_attributes == {
            "enable_replicated_table_tracker": True,
            "preferred_sync_replica_clusters": ["remote0", "remote1"],
            "min_sync_replica_count": 1,
            "max_sync_replica_count": 2,
            "enable_preload_check": True,
        }

        assert RttOptions.make(
            {
                "replicated_table_tracker_enabled": True,
                "min_sync_replica_count": None,
            }
        ).yt_attributes == {
            "enable_replicated_table_tracker": True,
            "min_sync_replica_count": None,
        }

        assert RttOptions.make({}).yt_attributes == {"enable_replicated_table_tracker": False}


class TestYtTable(YtReplicaTestBase):
    @pytest.fixture
    @staticmethod
    def table_attributes(
        default_schema: Types.Schema, replica_id: str, replica_attributes: Types.Attributes
    ) -> Types.Attributes:
        return {"schema": default_schema, "replicas": {replica_id: replica_attributes}}

    @pytest.mark.parametrize(
        "in_collocation", [True, False, None], ids=["in_collocation", "no_collocation", "implicit_no_collocation"]
    )
    @pytest.mark.parametrize(
        "table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE]
    )
    @pytest.mark.parametrize("rtt_enabled", [True, False, None], ids=["rtt_enabled", "rtt_disabled", "rtt_default"])
    @pytest.mark.parametrize("rtt_inside", [True, False], ids=["rtt_inside", "rtt_outside"])
    def test_make(
        self,
        table_path: str,
        default_schema: Types.Schema,
        in_collocation: bool | None,
        table_type: str,
        rtt_enabled: bool | None,
        rtt_inside: bool,
    ):
        attrs = {
            "my_attr": YsonEntity(),
            "schema": default_schema,
            "replication_collocation_id": str(uuid.uuid4()),
            "replication_card_id": str(uuid.uuid4()),
        }
        if in_collocation is not None:
            attrs["in_collocation"] = in_collocation
        if rtt_enabled is not None:
            if rtt_inside:
                attrs["replicated_table_options"] = {"enable_replicated_table_tracker": rtt_enabled}
            else:
                attrs["enable_replicated_table_tracker"] = rtt_enabled
        key = f"{table_path}_key"
        table = YtTable.make(key, "primary", table_type, table_path, True, attrs)
        assert key == table.key
        assert "primary" == table.cluster_name
        name = get_node_name(table_path)
        assert name == table.name
        assert table_path == table.path
        assert table_type == table.table_type
        assert table.exists is True
        assert 3 == len(table.schema.columns)
        assert "schema" not in table.attributes
        assert "replication_collocation_id" not in table.attributes
        assert "in_collocation" not in table.attributes
        assert "replication_card_id" not in table.attributes
        assert table.attributes["dynamic"] is True
        assert table.attributes["my_attr"] is None
        assert table.tablet_state.is_mounted
        assert "//tmp/db_folder" == table.folder
        if table.is_replicated:
            assert table.in_collocation is bool(in_collocation)
        else:
            assert table.in_collocation is False
        assert bool(table.replication_collocation_id) == table.is_replicated
        if YtTable.Type.CHAOS_REPLICATED_TABLE == table_type:
            assert table.chaos_replication_card_id is not None
            assert table.is_chaos_replicated
        else:
            assert table.chaos_replication_card_id is None
            assert not table.is_chaos_replicated
        assert table.is_temporary is False
        assert (bool(rtt_enabled) and table.is_replicated) == table.is_rtt_enabled
        assert "replicated_table_options" not in table.attributes
        assert table.rtt_options is not None
        assert bool(rtt_enabled) == table.rtt_options.enabled

    @pytest.mark.parametrize("table_type", YtNode.Type.all() + ["int64_type"])
    def test_make_with_bad_types(self, table_path: str, table_attributes: Types.Attributes, table_type: str):
        with pytest.raises(AssertionError):
            YtTable.make("k", "primary", table_type, table_path, True, table_attributes)

    def test_make_with_replicas(self, table_path: str, table_attributes: Types.Attributes):
        # replicas present for existing replicated tables
        for table_type in (YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE):
            table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
            assert table.replicas
        # replicas NOT present for non existing replicated tables
        for table_type in (YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE):
            table = YtTable.make("k", "primary", table_type, table_path, False, table_attributes)
            assert not table.replicas
        # replicas NOT present for existing data tables
        for table_type in (YtTable.Type.TABLE, YtTable.Type.REPLICATION_LOG):
            table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
            assert not table.replicas

    def test_make_explicit_schema(
        self, table_path: str, table_attributes: Types.Attributes, default_schema_broad_key: Types.Schema
    ):
        table = YtTable.make(
            "k",
            "primary",
            YtTable.Type.TABLE,
            table_path,
            True,
            table_attributes,
            explicit_schema=SchemaSpec.parse(default_schema_broad_key),
        )
        assert table.schema == YtSchema.parse(default_schema_broad_key)
        assert "schema" not in table.attributes.attributes

    def test_make_explicit_in_collocation(self, table_path: str, table_attributes: Types.Attributes):
        table_attributes["in_collocation"] = True
        table = YtTable.make(
            "k",
            "primary",
            YtTable.Type.REPLICATED_TABLE,
            table_path,
            True,
            table_attributes,
            explicit_in_collocation=False,
        )
        assert table.in_collocation is False
        assert "in_collocation" not in table.attributes.attributes

    @pytest.mark.parametrize("ensure_empty_schema", [True, False])
    def test_make_empty_schema(self, table_path: str, ensure_empty_schema: bool):
        if ensure_empty_schema:
            with pytest.raises(AssertionError):
                YtTable.make(
                    "k",
                    "primary",
                    YtTable.Type.REPLICATED_TABLE,
                    table_path,
                    True,
                    {},
                    ensure_empty_schema=ensure_empty_schema,
                )
        else:
            table = YtTable.make(
                "k",
                "primary",
                YtTable.Type.REPLICATED_TABLE,
                table_path,
                True,
                {},
                ensure_empty_schema=ensure_empty_schema,
            )
            assert table.path == table_path
            assert not table.schema.columns

    @pytest.mark.parametrize(
        "table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE]
    )
    @pytest.mark.parametrize("is_chaos", [True, False])
    @pytest.mark.parametrize("add_explicit_queue_attrs", [True, False])
    def test_make_fix_implicit_queue_attrs(
        self,
        table_path: str,
        ordered_schema: Types.Schema,
        table_type: YtTable.Type,
        is_chaos: bool,
        add_explicit_queue_attrs: bool,
    ):
        attributes = {"schema": ordered_schema}
        queue_options = QueueOptions(is_chaos=is_chaos, is_replicated=True)
        if add_explicit_queue_attrs:
            queue_options.preserve_tablet_index = False
            queue_options.has_preserve_tablet_index = True
            queue_options.commit_ordering = "weak"
            queue_options.has_commit_ordering = True

        table = YtTable.make("k", "primary", table_type, table_path, True, attributes, queue_options=queue_options)
        if add_explicit_queue_attrs:
            if is_chaos or table.is_replicated:
                assert table.attributes["preserve_tablet_index"] is False
            else:
                assert not table.attributes.has_value("preserve_tablet_index")
            assert table.attributes["commit_ordering"] == "weak"
        else:
            if is_chaos or table.is_replicated:
                assert table.attributes["preserve_tablet_index"] is True
            else:
                assert not table.attributes.has_value("preserve_tablet_index")
            assert table.attributes["commit_ordering"] == "strong"

    @pytest.mark.parametrize(
        "table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE]
    )
    @pytest.mark.parametrize("rtt_enabled", [True, False])
    def test_set_rtt_enabled_from_replicas(
        self, table_type: str, table_path: str, table_attributes: Types.Attributes, rtt_enabled: bool
    ):
        table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)

        if table.is_replicated:
            replica = next(iter(table.replicas.values()))
            replica.enable_replicated_table_tracker = rtt_enabled

        assert table.is_rtt_enabled is False

        table.set_rtt_enabled_from_replicas()

        if table.is_replicated:
            assert rtt_enabled == table.is_rtt_enabled
        else:
            assert table.is_rtt_enabled is False

    @pytest.mark.parametrize("rtt_enabled", [True, False], ids=["rtt_enabled", "rtt_disabled"])
    def test_min_max_sync_replica_count(self, table_path: str, table_attributes: Types.Attributes, rtt_enabled: bool):
        table = YtTable.make("k", "primary", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes)
        table.is_rtt_enabled = rtt_enabled

        if rtt_enabled:
            assert table.min_sync_replica_count == 1
            assert table.max_sync_replica_count == 1
        else:
            assert table.min_sync_replica_count == 0
            assert table.max_sync_replica_count == 0

        table.rtt_options.min_sync_replica_count = 2
        table.rtt_options.max_sync_replica_count = 3

        if rtt_enabled:
            assert table.min_sync_replica_count == 2
            assert table.max_sync_replica_count == 3
        else:
            assert table.min_sync_replica_count == 0
            assert table.max_sync_replica_count == 0

    def test_fill_replicas(self, table_path: str, table_attributes: Types.Attributes):
        replicas_dict = table_attributes.pop("replicas")
        table = YtTable.make("k", "primary", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes)
        assert not table.replicas
        table.fill_replicas(replicas_dict)
        assert table.replicas
        key = ("remote0", table_path)
        assert key in table.replicas

    @pytest.mark.parametrize(
        "table_type", [YtTable.Type.TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE, YtTable.Type.REPLICATION_LOG]
    )
    @pytest.mark.parametrize("is_ordered", [True, False], ids=["ordered", "sorted"])
    def test_is_chaos_replication_log_required(
        self,
        table_path: str,
        table_type: str,
        is_ordered: bool,
        default_schema: Types.Schema,
        ordered_schema: Types.Schema,
    ):
        table_attributes = {"dynamic": True}
        table_attributes["schema"] = ordered_schema if is_ordered else default_schema

        table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
        if YtTable.Type.TABLE == table_type and not is_ordered:
            assert table.is_chaos_replication_log_required
        else:
            assert not table.is_chaos_replication_log_required

    def test_add_replica(self, table_path: str, table_attributes: Types.Attributes):
        replica_data = table_attributes.pop("replicas")
        replica_id = sorted(replica_data)[0]
        replica_attrs = replica_data[replica_id]
        table = YtTable.make("k", "primary", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes)
        assert not table.replicas
        table.add_replica(YtReplica.make(replica_id, replica_attrs))
        assert table.replicas
        key = ("remote0", table_path)
        assert key in table.replicas
        assert replica_id == table.replicas[key].replica_id

    def test_add_replica_for_not_replicated(self, table_path: str, table_attributes: Types.Attributes):
        cluster = YtCluster.make(YtCluster.Type.MAIN, "primary", False)
        table = cluster.add_table_from_attributes("k", YtTable.Type.TABLE, table_path, True, table_attributes)
        with pytest.raises(AssertionError):
            table.add_replica_for(
                YtTable.make("k", "remote0", YtTable.Type.TABLE, table_path, True, table_attributes),
                cluster,
                YtReplica.Mode.ASYNC,
            )

    def test_add_replica_for_add_replicated(self, table_path: str, table_attributes: Types.Attributes):
        cluster = YtCluster.make(YtCluster.Type.MAIN, "primary", False)
        table = cluster.add_table_from_attributes(
            "k", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes
        )
        with pytest.raises(AssertionError):
            table.add_replica_for(
                YtTable.make("k", "remote0", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes),
                cluster,
                YtReplica.Mode.ASYNC,
            )

    def test_add_replica_for_self(self, table_path: str, table_attributes: Types.Attributes):
        cluster = YtCluster.make(YtCluster.Type.MAIN, "primary", False)
        table = cluster.add_table_from_attributes(
            "k", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes
        )
        with pytest.raises(AssertionError):
            table.add_replica_for(
                YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes),
                cluster,
                YtReplica.Mode.ASYNC,
            )

    @pytest.mark.parametrize("temporary", [True, False], ids=["temporary", "persistent"])
    @pytest.mark.parametrize("rtt_enabled", [True, False], ids=["rtt_enabled", "rtt_disabled"])
    def test_add_replica_for(
        self, table_path: str, table_attributes: Types.Attributes, temporary: bool, rtt_enabled: bool
    ):
        cluster = YtCluster.make(YtCluster.Type.MAIN, "primary", False)
        table = cluster.add_table_from_attributes(
            "k", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes
        )

        replica_cluster = YtCluster.make(YtCluster.Type.REPLICA, "remote0", False, YtCluster.Mode.SYNC)
        replica_table = replica_cluster.add_table_from_attributes(
            "k", YtTable.Type.TABLE, table_path, True, table_attributes
        )
        replica_table.is_temporary = temporary
        replica_table.rtt_options.enabled = rtt_enabled
        table.add_replica_for(replica_table, replica_cluster, YtReplica.Mode.SYNC)

        assert 1 == len(table.replicas)
        replica = list(table.replicas.values())[0]
        assert "remote0" == replica.cluster_name
        assert table_path == replica.replica_path
        assert YtReplica.Mode.SYNC == replica.mode
        assert temporary == replica.is_temporary
        assert replica_table.is_replicated is False
        assert rtt_enabled == replica.enable_replicated_table_tracker

    @pytest.mark.parametrize("temporary", [True, False], ids=["temporary", "persistent"])
    @pytest.mark.parametrize("rtt_enabled", [True, False], ids=["rtt_enabled", "rtt_disabled"])
    def test_add_replica_for_chaos(
        self, table_path: str, table_attributes: Types.Attributes, temporary: bool, rtt_enabled: bool
    ):
        cluster = YtCluster.make(YtCluster.Type.MAIN, "primary", True)
        table = cluster.add_table_from_attributes(
            "k", YtTable.Type.CHAOS_REPLICATED_TABLE, table_path, True, table_attributes
        )

        replica_cluster = YtCluster.make(YtCluster.Type.REPLICA, "remote0", True, YtCluster.Mode.SYNC)
        replica_table = replica_cluster.add_table_from_attributes(
            "k", YtTable.Type.TABLE, table_path, True, table_attributes
        )
        replica_table.is_temporary = temporary
        replica_table.rtt_options.enabled = rtt_enabled
        replication_log = replica_cluster.add_replication_log_for(replica_table)
        assert replication_log
        replication_log.is_temporary = temporary
        table.add_replica_for(replica_table, replica_cluster, YtReplica.Mode.SYNC)

        assert replica_table.is_rtt_enabled is False
        assert replication_log.is_rtt_enabled is False

        assert 2 == len(table.replicas)
        for replica in table.replicas.values():
            assert "remote0" == replica.cluster_name
            assert YtReplica.Mode.SYNC == replica.mode
            assert replica.content_type in (YtReplica.ContentType.DATA, YtReplica.ContentType.QUEUE)
            assert temporary == replica.is_temporary
            assert rtt_enabled == replica.enable_replicated_table_tracker

    def test_get_replicas_for(self, table_path: str, table_attributes: Types.Attributes):
        cluster = YtCluster.make(YtCluster.Type.MAIN, "primary", False)
        main_table = cluster.add_table_from_attributes(
            "k", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes
        )

        replica_cluster_0 = YtCluster.make(YtCluster.Type.REPLICA, "remote0", False)
        replica_table_0 = replica_cluster_0.add_table_from_attributes(
            "k", YtTable.Type.TABLE, table_path, True, table_attributes
        )
        main_table.add_replica_for(replica_table_0, replica_cluster_0, YtReplica.Mode.ASYNC)

        replica_cluster_1 = YtCluster.make(YtCluster.Type.REPLICA, "remote1", False)
        replica_table_1 = replica_cluster_1.add_table_from_attributes(
            "k", YtTable.Type.TABLE, table_path, True, table_attributes
        )
        main_table.add_replica_for(replica_table_1, replica_cluster_1, YtReplica.Mode.ASYNC)

        replicas_cluster_0 = main_table.get_replicas_for("remote0")
        assert 1 == len(replicas_cluster_0)
        assert "remote0" == replicas_cluster_0[0].cluster_name

        replicas_cluster_1 = main_table.get_replicas_for("remote1")
        assert 1 == len(replicas_cluster_1)
        assert "remote1" == replicas_cluster_1[0].cluster_name

        assert 0 == len(main_table.get_replicas_for("primary"))

    def test_sync_replicas_mode_not_replicated(self, table_path: str, table_attributes: Types.Attributes):
        table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)
        with pytest.raises(AssertionError):
            table.sync_replicas_mode(set())

    @pytest.mark.parametrize("rtt_enabled", [True, False], ids=["rtt_enabled", "rtt_disabled"])
    def test_sync_replicas_mode(self, table_path: str, table_attributes: Types.Attributes, rtt_enabled: bool):
        table = YtTable.make("k", "primary", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes)

        table.add_replica(make_replica("remote0", table_path, YtReplica.Mode.SYNC, rtt_enabled))
        table.add_replica(make_replica("remote1", table_path, YtReplica.Mode.ASYNC, rtt_enabled))

        # make remote0 async and remote1 sync
        table.sync_replicas_mode({"remote1"})

        assert YtReplica.Mode.ASYNC == table.get_replicas_for("remote0")[0].mode
        assert YtReplica.Mode.SYNC == table.get_replicas_for("remote1")[0].mode

        if rtt_enabled:
            assert {"remote1"} == table.rtt_options.preferred_sync_replica_clusters

        # make all async
        table.sync_replicas_mode(set())

        assert YtReplica.Mode.ASYNC == table.get_replicas_for("remote0")[0].mode
        assert YtReplica.Mode.ASYNC == table.get_replicas_for("remote1")[0].mode

        assert not table.rtt_options.preferred_sync_replica_clusters

    @pytest.mark.parametrize("rtt_enabled", [True, False], ids=["rtt_enabled", "rtt_disabled"])
    @pytest.mark.parametrize(
        "table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE]
    )
    def test_sync_replicas(
        self, table_path: str, table_attributes: Types.Attributes, rtt_enabled: bool, table_type: str
    ):
        table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)

        for cluster in ("remote0", "remote1"):
            mode = YtReplica.Mode.SYNC if "remote0" == cluster else YtReplica.Mode.ASYNC
            table_replica = make_replica(cluster, table_path, mode, rtt_enabled)
            if YtTable.Type.CHAOS_REPLICATED_TABLE == table_type:
                table_replica.content_type = YtReplica.ContentType.DATA
                log_replica = make_replica(
                    cluster, make_log_name(table_path), YtReplica.Mode.SYNC if rtt_enabled else mode, rtt_enabled
                )
                log_replica.content_type = YtReplica.ContentType.QUEUE
                table.add_replica(log_replica)
            table.add_replica(table_replica)

        if YtTable.Type.TABLE == table_type:
            assert not table.sync_replicas
        else:
            assert set(["remote0"]) == table.sync_replicas

    @pytest.mark.parametrize("rtt_enabled", [True, False], ids=["rtt_enabled", "rtt_disabled"])
    @pytest.mark.parametrize(
        "table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE]
    )
    def test_sync_replicas_ordered(
        self, table_path: str, ordered_schema: Types.Schema, rtt_enabled: bool, table_type: str
    ):
        table = YtTable.make("k", "primary", table_type, table_path, True, {"dynamic": True, "schema": ordered_schema})
        for cluster in ("remote0", "remote1"):
            mode = YtReplica.Mode.SYNC if "remote0" == cluster else YtReplica.Mode.ASYNC
            table_replica = make_replica(cluster, table_path, mode, rtt_enabled)
            if YtTable.Type.CHAOS_REPLICATED_TABLE == table_type:
                table_replica.content_type = YtReplica.ContentType.QUEUE
            table.add_replica(table_replica)
        if YtTable.Type.TABLE == table_type:
            assert not table.sync_replicas
        else:
            assert set(["remote0"]) == table.sync_replicas

    def test_get_chaos_replica_content_type(
        self, table_path: str, default_schema: Types.Schema, ordered_schema: Types.Schema
    ):
        attrs = {"schema": default_schema}

        for content_type, table_type in zip(
            [YtReplica.ContentType.DATA, YtReplica.ContentType.QUEUE],
            [YtTable.Type.TABLE, YtTable.Type.REPLICATION_LOG],
        ):
            assert (
                content_type
                == YtTable.make("k", "primary", table_type, table_path, True, attrs).chaos_replica_content_type
            )

        for table_type in (YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE):
            with pytest.raises(NotImplementedError):
                YtTable.make(
                    "k", "primary", YtTable.Type.REPLICATED_TABLE, table_path, True, attrs
                ).chaos_replica_content_type

        # ordered table
        assert (
            YtReplica.ContentType.QUEUE
            == YtTable.make(
                "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": ordered_schema}
            ).chaos_replica_content_type
        )

    def test_resolve_table_type(self):
        assert YtTable.Type.TABLE == YtTable.resolve_table_type(YtTable.Type.TABLE, True)
        assert YtTable.Type.TABLE == YtTable.resolve_table_type(YtTable.Type.TABLE, False)
        assert YtTable.Type.REPLICATED_TABLE == YtTable.resolve_table_type(YtTable.Type.REPLICATED_TABLE, False)
        assert YtTable.Type.CHAOS_REPLICATED_TABLE == YtTable.resolve_table_type(YtTable.Type.REPLICATED_TABLE, True)

    def test_resolve_replication_log_attributes(self, default_schema: Types.Schema):
        attrs = {
            "replication_log": {"attr1": 1},
            "tablet_cell_bundle": "grut",
            "primary_medium": "ssd",
            "schema": default_schema,
            "attr2": 2,
            "enable_replicated_table_tracker": True,
        }
        expected_attrs = {
            "attr1": 1,
            "tablet_cell_bundle": "grut",
            "primary_medium": "ssd",
            "schema": default_schema,
            "enable_replicated_table_tracker": True,
        }
        assert expected_attrs == YtTable.resolve_replication_log_attributes(attrs)

    @pytest.mark.parametrize(
        "is_replicated,table_type",
        [
            (True, YtTable.Type.REPLICATED_TABLE),
            (True, YtTable.Type.CHAOS_REPLICATED_TABLE),
            (False, YtTable.Type.TABLE),
            (False, YtTable.Type.REPLICATION_LOG),
        ],
    )
    def test_is_replicated_type(self, is_replicated: bool, table_type: str):
        assert is_replicated == YtTable.is_replicated_type(table_type)

    @pytest.mark.parametrize(
        "is_replicated,table_type",
        [
            (True, YtTable.Type.REPLICATED_TABLE),
            (True, YtTable.Type.CHAOS_REPLICATED_TABLE),
            (False, YtTable.Type.TABLE),
            (False, YtTable.Type.REPLICATION_LOG),
        ],
    )
    def test_is_replicated(
        self, table_path: str, table_attributes: Types.Attributes, is_replicated: bool, table_type: str
    ):
        assert (
            is_replicated == YtTable.make("k", "primary", table_type, table_path, True, table_attributes).is_replicated
        )

    @pytest.mark.parametrize("table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATED_TABLE])
    def test_yt_attributes(self, table_path: str, default_schema: Types.Schema, table_type: str):
        collocation_id = str(uuid.uuid4())
        attrs = {"my_attr": YsonEntity(), "schema": default_schema, "replication_collocation_id": collocation_id}
        table = YtTable.make("k", "primary", table_type, table_path, True, attrs)
        expected = {
            "dynamic": True,
            "schema": YtSchema.parse(default_schema).yt_schema,
        }
        if YtTable.Type.REPLICATED_TABLE == table_type:
            expected["replication_collocation_id"] = collocation_id
            expected["replicated_table_options"] = {}
        assert expected == table.yt_attributes

    @pytest.mark.parametrize("table_type", [YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE])
    @pytest.mark.parametrize("rtt_enabled", [True, False], ids=["rtt_enabled", "rtt_disabled"])
    def test_yt_attributes_with_rtt(
        self, table_path: str, default_schema: Types.Schema, table_type: str, rtt_enabled: bool
    ):
        table = YtTable.make("k", "primary", table_type, table_path, True, {"schema": default_schema})
        table.is_rtt_enabled = rtt_enabled
        table.fill_replicas(
            {
                "r1": {
                    "cluster_name": "remote0",
                    "replica_path": table_path,
                    "mode": YtReplica.Mode.SYNC,
                    "state": "enabled",
                    "replicated_table_tracker_enabled": True,
                }
            }
        )
        expected = {
            "schema": YtSchema.parse(default_schema).yt_schema,
        }
        if rtt_enabled:
            expected["replicated_table_options"] = {
                "enable_replicated_table_tracker": rtt_enabled,
                "preferred_sync_replica_clusters": ["remote0"],
            }
        else:
            expected["replicated_table_options"] = dict()
        if YtTable.Type.REPLICATED_TABLE == table_type:
            expected["dynamic"] = True
        assert expected == table.yt_attributes

    @pytest.mark.parametrize("table_type", YtTable.Type.all())
    @pytest.mark.parametrize("tablets_mode", ["tablet_count", "pivot_keys", None])
    def test_get_full_spec_sorted(
        self, table_path: str, default_schema: Types.Schema, table_type: str, tablets_mode: str | None
    ):
        attrs = {"schema": default_schema}
        expected = {"schema": YtSchema.parse(default_schema).yt_schema}
        if table_type != YtTable.Type.CHAOS_REPLICATED_TABLE:
            expected["dynamic"] = True
        if YtTable.is_replicated_type(table_type):
            expected["replicated_table_options"] = dict()
        match tablets_mode:
            case "tablet_count":
                attrs["tablet_count"] = 2
                if table_type != YtTable.Type.CHAOS_REPLICATED_TABLE:
                    expected["tablet_count"] = 2
            case "pivot_keys":
                attrs["pivot_keys"] = [[], [10]]
                if table_type != YtTable.Type.CHAOS_REPLICATED_TABLE:
                    expected["pivot_keys"] = [[], [10]]
        table = YtTable.make("k", "primary", table_type, table_path, True, attrs)
        assert expected == table.get_full_spec()

    @pytest.mark.parametrize("table_type", YtTable.Type.all())
    @pytest.mark.parametrize("tablets_mode", ["tablet_count", "pivot_keys", None])
    def test_get_full_spec_ordered(
        self, table_path: str, ordered_schema: Types.Schema, table_type: str, tablets_mode: str | None
    ):
        attrs = {"schema": ordered_schema}
        expected = {"schema": YtSchema.parse(ordered_schema).yt_schema}
        if table_type != YtTable.Type.CHAOS_REPLICATED_TABLE:
            expected["dynamic"] = True
        if YtTable.is_replicated_type(table_type):
            expected["replicated_table_options"] = dict()
        match tablets_mode:
            case "tablet_count":
                attrs["tablet_count"] = 2
                if table_type != YtTable.Type.CHAOS_REPLICATED_TABLE:
                    expected["tablet_count"] = 2
            case "pivot_keys":
                attrs["pivot_keys"] = [[], [10]]
        table = YtTable.make("k", "primary", table_type, table_path, True, attrs)
        assert expected == table.get_full_spec()

    @pytest.mark.parametrize("table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATION_LOG])
    def test_replica_type_fail(self, table_path: str, table_attributes: Types.Attributes, table_type: str):
        with pytest.raises(AssertionError):
            YtTable.make("k", "primary", table_type, table_path, True, table_attributes).replica_type

    @pytest.mark.parametrize(
        "replica_type,table_type",
        [
            ("table_replica", YtTable.Type.REPLICATED_TABLE),
            ("chaos_table_replica", YtTable.Type.CHAOS_REPLICATED_TABLE),
        ],
    )
    def test_replica_type(
        self, table_path: str, table_attributes: Types.Attributes, replica_type: str, table_type: str
    ):
        assert replica_type == YtTable.make("k", "primary", table_type, table_path, True, table_attributes).replica_type

    def test_effective_tablet_count_sorted(self, table_path: str, default_schema: Types.Schema):
        assert (
            YtTable.make(
                "k", "primary", YtTable.Type.TABLE, table_path, True, {"dynamic": True, "schema": default_schema}
            ).effective_tablet_count
            == 1
        )
        assert (
            YtTable.make(
                "k",
                "primary",
                YtTable.Type.TABLE,
                table_path,
                True,
                {"dynamic": True, "schema": default_schema, "tablet_count": 3},
            ).effective_tablet_count
            == 3
        )
        assert (
            YtTable.make(
                "k",
                "primary",
                YtTable.Type.TABLE,
                table_path,
                True,
                {"dynamic": True, "schema": default_schema, "pivot_keys": [[], [1], [2]]},
            ).effective_tablet_count
            == 3
        )
        assert (
            YtTable.make(
                "k",
                "primary",
                YtTable.Type.TABLE,
                table_path,
                True,
                {"dynamic": True, "schema": default_schema, "tablet_count": 1, "pivot_keys": [[], [1], [2]]},
            ).effective_tablet_count
            == 3
        )

    def test_effective_tablet_count_ordered(self, table_path: str, ordered_schema: Types.Schema):
        # no tablet count or pivot keys -> effective_tablet_count=1
        assert (
            YtTable.make(
                "k", "primary", YtTable.Type.TABLE, table_path, True, {"dynamic": True, "schema": ordered_schema}
            ).effective_tablet_count
            == 1
        )
        # effective_tablet_count=tablet_count
        assert (
            YtTable.make(
                "k",
                "primary",
                YtTable.Type.TABLE,
                table_path,
                True,
                {"dynamic": True, "schema": ordered_schema, "tablet_count": 3},
            ).effective_tablet_count
            == 3
        )
        # pivot_keys not supported for ordered
        assert (
            YtTable.make(
                "k",
                "primary",
                YtTable.Type.TABLE,
                table_path,
                True,
                {"dynamic": True, "schema": ordered_schema, "pivot_keys": [[], [1], [2]]},
            ).effective_tablet_count
            == 1
        )
        # prefer tablet_count over pivot_keys for ordered
        assert (
            YtTable.make(
                "k",
                "primary",
                YtTable.Type.TABLE,
                table_path,
                True,
                {"dynamic": True, "schema": ordered_schema, "tablet_count": 2, "pivot_keys": [[], [1], [2]]},
            ).effective_tablet_count
            == 2
        )

    def test_has_tablets_default(self, table_path: str, table_attributes: Types.Attributes):
        table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)
        assert not table.has_pivot_keys
        assert not table.has_tablets

    def test_has_tablets_empty_pivot_keys(self, table_path: str, table_attributes: Types.Attributes):
        table_attributes["pivot_keys"] = [[]]
        table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)
        assert not table.has_pivot_keys
        assert not table.has_tablets

    def test_has_tablets_pivot_keys(self, table_path: str, table_attributes: Types.Attributes):
        table_attributes["pivot_keys"] = [[], 1, 2]
        table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)
        assert table.has_pivot_keys
        assert table.has_tablets

    def test_has_tablets_tablet_count(self, table_path: str, table_attributes: Types.Attributes):
        table_attributes["tablet_count"] = 1
        table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)
        assert not table.has_pivot_keys
        assert table.has_tablets

    @pytest.mark.parametrize("in_memory_mode", ["compressed", "uncompressed", "none"])
    def test_is_in_memory(self, table_path: str, table_attributes: Types.Attributes, in_memory_mode: str):
        table_attributes["in_memory_mode"] = in_memory_mode
        table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)
        assert table.is_in_memory == bool(in_memory_mode != "none")

    @pytest.mark.parametrize("with_hunks", [True, False])
    def test_has_hunks(self, table_path: str, default_schema: Types.Schema, with_hunks: bool):
        new_column = {"name": "BigStringValue", "type": "string"}
        if with_hunks:
            new_column["max_inline_hunk_size"] = 10
        default_schema.append(new_column)
        table = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"dynamic": True, "schema": default_schema}
        )
        assert with_hunks == table.has_hunks

    @pytest.mark.parametrize("table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATED_TABLE])
    def test_rtt_enabled_replicas(
        self, table_path: str, table_attributes: Types.Attributes, table_type: str, replica_id: str
    ):
        table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
        replica_id2 = str(uuid.uuid4())
        table.add_replica(
            YtReplica.make(
                replica_id2,
                {
                    "cluster_name": "remote1",
                    "replica_path": table_path,
                    "mode": YtReplica.Mode.ASYNC,
                    "state": "enabled",
                    "enable_replicated_table_tracker": False,
                },
            )
        )
        seen_replicas = [r for r in table.rtt_enabled_replicas]
        if table.is_replicated:
            assert 1 == len(seen_replicas)
            assert replica_id == seen_replicas[0].replica_id
            assert "remote0" == seen_replicas[0].cluster_name
        else:
            assert not seen_replicas

    @pytest.mark.parametrize("table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATED_TABLE])
    def test_preferred_sync_replicas(self, table_path: str, table_attributes: Types.Attributes, table_type: str):
        table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
        table.add_replica(
            YtReplica.make(
                str(uuid.uuid4()),
                {
                    "cluster_name": "remote1",
                    "replica_path": table_path,
                    "mode": YtReplica.Mode.ASYNC,
                    "state": "enabled",
                },
            )
        )
        if table.is_replicated:
            assert {"remote0"} == table.preferred_sync_replicas
        else:
            assert not table.preferred_sync_replicas

    def test_preferred_sync_replicas_chaos(self, table_path: str, table_attributes: Types.Attributes):
        table = YtTable.make("k", "primary", YtTable.Type.CHAOS_REPLICATED_TABLE, table_path, True, table_attributes)
        table.add_replica(
            YtReplica.make(
                str(uuid.uuid4()),
                {
                    "cluster_name": "remote1",
                    "replica_path": table_path,
                    "mode": YtReplica.Mode.SYNC,
                    "state": "enabled",
                    "content_type": YtReplica.ContentType.QUEUE,
                    "enable_replicated_table_tracker": True,
                },
            )
        )
        assert {"remote0"} == table.preferred_sync_replicas

    @pytest.mark.parametrize("table_type", [YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE])
    def test_preferred_sync_replicas_ordered(
        self,
        table_path: str,
        ordered_schema: Types.Schema,
        replica_attributes: Types.Attributes,
        replica_id: str,
        table_type: str,
    ):
        replica_attributes["content_type"] = YtReplica.ContentType.QUEUE
        table_attributes = {"schema": ordered_schema, "replicas": {replica_id: replica_attributes}}
        table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
        table.add_replica(
            YtReplica.make(
                str(uuid.uuid4()),
                {
                    "cluster_name": "remote1",
                    "replica_path": table_path,
                    "mode": YtReplica.Mode.SYNC,
                    "state": "enabled",
                    "content_type": YtReplica.ContentType.QUEUE,
                    "enable_replicated_table_tracker": True,
                },
            )
        )
        assert {"remote0", "remote1"} == table.preferred_sync_replicas

    @pytest.mark.parametrize("replica_rtt_enabled", [True, False])
    def test_has_rtt_enabled_replicas(
        self, table_path: str, table_attributes: Types.Attributes, replica_rtt_enabled: bool
    ):
        table = YtTable.make("k", "primary", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes)
        replica = next(iter(table.replicas.values()))
        replica.enable_replicated_table_tracker = replica_rtt_enabled
        assert replica_rtt_enabled == table.has_rtt_enabled_replicas

    def test_is_ordered(self, table_path: str, default_schema: Types.Schema, ordered_schema: Types.Schema):
        assert not YtTable.make("k", "p", YtTable.Type.TABLE, table_path, True, {"schema": default_schema}).is_ordered
        assert YtTable.make("k", "p", YtTable.Type.TABLE, table_path, True, {"schema": ordered_schema}).is_ordered

    @pytest.mark.parametrize("is_consumer", [False, True])
    def test_is_consumer(self, table_path: str, is_consumer: bool, default_schema: Types.Attributes):
        table_attributes = CONSUMER_ATTRS if is_consumer else {"dynamic": True}
        table_attributes["schema"] = CONSUMER_SCHEMA if is_consumer else default_schema

        table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)
        assert table.is_consumer == is_consumer

    @pytest.mark.parametrize("is_ordered,is_consumer", [(True, False), (False, True), (False, False)])
    def test_is_data_table(
        self,
        table_path: str,
        default_schema: Types.Schema,
        ordered_schema: Types.Schema,
        is_ordered: bool,
        is_consumer: bool,
    ):
        table_attributes = CONSUMER_ATTRS if is_consumer else {"dynamic": True}
        if is_consumer:
            table_attributes["schema"] = CONSUMER_SCHEMA
        elif is_ordered:
            table_attributes["schema"] = ordered_schema
        else:
            table_attributes["schema"] = default_schema
        table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)
        assert table.is_data_table != is_ordered
        if is_consumer:
            assert table.is_data_table

    @pytest.mark.parametrize(
        "table_type",
        [
            YtTable.Type.TABLE,
            YtTable.Type.REPLICATED_TABLE,
            YtTable.Type.REPLICATION_LOG,
            YtTable.Type.CHAOS_REPLICATED_TABLE,
        ],
    )
    def test_is_mountable(self, table_path: str, table_attributes: Types.Attributes, table_type: str):
        table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
        assert table.is_mountable == (table_type != YtTable.Type.CHAOS_REPLICATED_TABLE)

    @pytest.mark.parametrize("table_type", [YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE])
    def test_to_data_table(self, table_type: str, table_path: str, table_attributes: Types.Attributes):
        table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
        assert table.replicas

        table.to_data_table()
        assert not table.replicas
        assert YtTable.Type.TABLE == table.table_type

    def test_to_data_table_not_replicated(self, table_path: str, table_attributes: Types.Attributes):
        table = YtTable.make("k", "remote0", YtTable.Type.REPLICATION_LOG, table_path, True, table_attributes)
        table.to_data_table()
        assert YtTable.Type.REPLICATION_LOG == table.table_type

    def test_sync_user_attributes(self, table_path: str, table_attributes: Types.Attributes):
        attributes1 = deepcopy(table_attributes)
        attributes1.update(
            {
                "my_attr1": "val1",
                "user_attribute_keys": ["my_attr1"],
            }
        )
        attribute2 = deepcopy(table_attributes)
        attribute2.update(
            {
                "my_attr1": "val2",
                "my_attr2": "val3",
                "my_attr3": None,
                "user_attribute_keys": ["my_attr1", "my_attr2", "my_attr3"],
            }
        )

        table1 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, attributes1)
        table2 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, attribute2)

        table1.sync_user_attributes(table2)
        table_attrs = table1.attributes
        assert set(["my_attr1", "my_attr2"]) == table_attrs.user_attribute_keys
        assert "val1" == table_attrs["my_attr1"]
        assert "val3" == table_attrs["my_attr2"]
        assert "my_attr3" not in table_attrs

    def test_effective_max_sync_replica_count(self, table_path: str, table_attributes: Types.Attributes):
        simple_table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)
        with pytest.raises(Exception):
            simple_table.effective_max_sync_replica_count

        table = YtTable.make("k", "primary", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes)
        assert table.effective_max_sync_replica_count == 1

        table.replicas = [1, 2]  # Property just takes length of replicas.
        assert table.effective_max_sync_replica_count == 1

        table.rtt_options.min_sync_replica_count = 3
        # attributes["replicated_table_options"] = {"min_sync_replica_count": 3}
        assert table.effective_max_sync_replica_count == len(table.replicas)

        table.rtt_options.max_sync_replica_count = 1
        # attributes["replicated_table_options"]["max_sync_replica_count"] = 1
        assert table.effective_max_sync_replica_count == 1

    def test_link_replication_log_bad_type(self, table_path: str, table_attributes: Types.Attributes):
        table = YtTable.make("k", "p", YtTable.Type.TABLE, table_path, True, table_attributes)
        log = YtTable.make(
            make_log_name(table.key), "p", YtTable.Type.REPLICATION_LOG, table_path, True, table_attributes
        )

        for table_type in (
            YtTable.Type.CHAOS_REPLICATED_TABLE,
            YtTable.Type.REPLICATED_TABLE,
            YtTable.Type.REPLICATION_LOG,
        ):
            table.table_type = table_type
            with pytest.raises(AssertionError):
                table.link_replication_log(log)

        table.table_type = YtTable.Type.TABLE
        for log_type in (YtTable.Type.CHAOS_REPLICATED_TABLE, YtTable.Type.REPLICATED_TABLE, YtTable.Type.TABLE):
            log.table_type = log_type
            with pytest.raises(AssertionError):
                table.link_replication_log(log)

    def test_link_replication_log_bad_key(self, table_path: str, table_attributes: Types.Attributes):
        table = YtTable.make("k", "p", YtTable.Type.TABLE, table_path, True, table_attributes)
        log = YtTable.make("_some_key_", "p", YtTable.Type.REPLICATION_LOG, table_path, True, table_attributes)
        with pytest.raises(AssertionError):
            table.link_replication_log(log)

    def test_link_replication_log(self, table_path: str, table_attributes: Types.Attributes):
        table = YtTable.make("k", "p", YtTable.Type.TABLE, table_path, True, table_attributes)
        log = YtTable.make(
            make_log_name(table.key), "p", YtTable.Type.REPLICATION_LOG, table_path, True, table_attributes
        )
        table.link_replication_log(log)

        assert table.chaos_replication_log == log.key
        assert log.chaos_data_table == table.key
