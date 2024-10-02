from typing import Any
from typing import TypeAlias

import pytest

from yt.yt_sync.core.spec.details import SchemaSpec
from yt.yt_sync.core.spec.details import TableSpec


class Types:
    Schema: TypeAlias = list[dict[str, str | bool | int]]


class TestTableSpecification:
    def test_parse_empty(self):
        with pytest.raises(AssertionError):
            TableSpec.parse({})

    @pytest.mark.parametrize("value", [1, 1.0, "1", True, []])
    def test_parse_non_dict(self, value: Any):
        with pytest.raises(AssertionError):
            TableSpec.parse(value)

    def test_parse_no_clusters(self, default_schema: Types.Schema):
        with pytest.raises(AssertionError):
            TableSpec.parse({"schema": default_schema})

    def test_parse_empty_clusters(self, default_schema: Types.Schema):
        with pytest.raises(AssertionError):
            TableSpec.parse({"schema": default_schema, "clusters": {}})

    def test_parse_no_path_for_cluster(self, default_schema: Types.Schema):
        with pytest.raises(AssertionError):
            TableSpec.parse({"schema": default_schema, "clusters": {"primary": {"attributes": {}}}})

    def test_empty_cluster_name(self, table_path: str, default_schema: Types.Schema):
        with pytest.raises(AssertionError):
            TableSpec.parse({"schema": default_schema, "clusters": {"": {"path": table_path}}})

    def test_no_schema(self, table_path: str):
        with pytest.raises(AssertionError):
            TableSpec.parse({"clusters": {"primary": {"path": table_path}}})

    def test_empty_schema(self, table_path: str):
        with pytest.raises(AssertionError):
            TableSpec.parse({"schema": [], "clusters": {"primary": {"path": table_path}}})

    def test_bad_schema_override(self, table_path: str, default_schema: Types.Schema):
        with pytest.raises(AssertionError):
            TableSpec.parse(
                {
                    "schema": default_schema,
                    "clusters": {
                        "primary": {"main": True, "path": table_path},
                        "remote0": {"main": False, "path": table_path, "schema_override": [{}]},
                    },
                }
            )

    @pytest.mark.parametrize("in_collocation", [True, False])
    def test_in_collocation_for_single(self, table_path: str, default_schema: Types.Schema, in_collocation: bool):
        with pytest.raises(AssertionError):
            TableSpec.parse(
                {
                    "schema": default_schema,
                    "in_collocation": in_collocation,
                    "clusters": {"primary": {"path": table_path}},
                }
            )

    def test_no_main(self, table_path: str, default_schema: Types.Schema):
        with pytest.raises(AssertionError):
            TableSpec.parse(
                {
                    "schema": default_schema,
                    "clusters": {
                        "primary": {"path": table_path},
                        "remote0": {"path": table_path},
                    },
                }
            )

    def test_many_main(self, table_path: str, default_schema: Types.Schema):
        with pytest.raises(AssertionError):
            TableSpec.parse(
                {
                    "schema": default_schema,
                    "clusters": {
                        "primary": {"main": True, "path": table_path},
                        "remote0": {"main": True, "path": table_path},
                    },
                }
            )

    @pytest.mark.parametrize("rtt", [True, False])
    def test_rtt_for_main(self, table_path: str, default_schema: Types.Schema, rtt: bool):
        with pytest.raises(AssertionError):
            TableSpec.parse(
                {
                    "schema": default_schema,
                    "clusters": {
                        "primary": {"main": True, "path": table_path, "replicated_table_tracker_enabled": rtt},
                        "remote0": {"path": table_path},
                    },
                }
            )

    @pytest.mark.parametrize("sync", [True, False])
    def test_preferred_sync_for_main(self, table_path: str, default_schema: Types.Schema, sync: bool):
        with pytest.raises(AssertionError):
            TableSpec.parse(
                {
                    "schema": default_schema,
                    "clusters": {
                        "primary": {"main": True, "path": table_path, "preferred_sync": sync},
                        "remote0": {"path": table_path},
                    },
                }
            )

    @pytest.mark.parametrize("chaos", [True, False])
    def test_replication_log_for_name(self, table_path: str, default_schema: Types.Schema, chaos: bool):
        with pytest.raises(AssertionError):
            TableSpec.parse(
                {
                    "chaos": chaos,
                    "schema": default_schema,
                    "clusters": {
                        "primary": {"main": True, "path": table_path, "replication_log": {}},
                        "remote0": {"path": table_path},
                    },
                }
            )

    def test_replication_log_non_chaos_replica(self, table_path: str, default_schema: Types.Schema):
        with pytest.raises(AssertionError):
            TableSpec.parse(
                {
                    "chaos": False,
                    "schema": default_schema,
                    "clusters": {
                        "primary": {"main": True, "path": table_path},
                        "remote0": {"path": table_path, "replication_log": {}},
                    },
                }
            )

    def test_parse_single_clusters(self, table_path: str, default_schema: Types.Schema):
        attrs = {"dynamic": True}
        spec: TableSpec = TableSpec.parse(
            {"schema": default_schema, "clusters": {"primary": {"path": table_path, "attributes": attrs}}}
        )
        assert not spec.chaos
        assert not spec.in_collocation
        assert not spec.is_federation
        assert spec.schema == SchemaSpec.parse(default_schema)

        assert "primary" in spec.clusters
        assert "primary" == spec.main_cluster

        cluster_spec = spec.clusters[spec.main_cluster]
        assert cluster_spec.main is True
        assert cluster_spec.path == table_path
        assert cluster_spec.replicated_table_tracker_enabled is None
        assert cluster_spec.preferred_sync is None
        assert cluster_spec.attributes == attrs
        assert cluster_spec.replication_log is None

    def test_parse_replicated_federation(self, table_path: str, default_schema: Types.Schema):
        cluster2path = {
            "primary": table_path,
            "remote0": f"{table_path}_0",
            "remote1": f"{table_path}_1",
            "remote2": f"{table_path}_2",
        }
        cluster2attrs = {
            "primary": {"dynamic": True},
            "remote0": {"dynamic": True, "my_attr0": "value0"},
            "remote1": {"dynamic": True, "my_attr1": "value1"},
            "remote2": {"dynamic": True, "my_attr2": "value2"},
        }
        cluster2patch = {
            "primary": {"main": True},
            "remote0": {"replicated_table_tracker_enabled": True, "preferred_sync": True},
            "remote1": {"replicated_table_tracker_enabled": True, "preferred_sync": False},
            "remote2": {},
        }

        def _cluster_spec(cluster: str) -> dict[str, Any]:
            base = {
                "path": cluster2path[cluster],
                "attributes": cluster2attrs[cluster],
            }
            base.update(cluster2patch[cluster])
            return base

        spec: TableSpec = TableSpec.parse(
            {
                "schema": default_schema,
                "in_collocation": True,
                "clusters": {
                    "primary": _cluster_spec("primary"),
                    "remote0": _cluster_spec("remote0"),
                    "remote1": _cluster_spec("remote1"),
                    "remote2": _cluster_spec("remote2"),
                },
            }
        )

        assert not spec.chaos
        assert spec.in_collocation
        assert spec.is_federation
        assert spec.schema == SchemaSpec.parse(default_schema)

        for cluster_name in cluster2path:
            assert cluster_name in spec.clusters
            cluster_spec = spec.clusters[cluster_name]
            if cluster_name == "primary":
                assert cluster_spec.main
                assert not cluster_spec.replicated_table_tracker_enabled
                assert not cluster_spec.preferred_sync
            else:
                assert not cluster_spec.main
                if cluster_name in ("remote0", "remote1"):
                    assert cluster_spec.replicated_table_tracker_enabled
                else:
                    assert not cluster_spec.replicated_table_tracker_enabled
                if cluster_name == "remote0":
                    assert cluster_spec.preferred_sync
                else:
                    assert not cluster_spec.preferred_sync
            assert cluster_spec.path == cluster2path[cluster_name]
            assert cluster_spec.attributes == cluster2attrs[cluster_name]
            assert cluster_spec.replication_log is None

    def test_parse_chaos_federation(self, table_path: str, default_schema: Types.Schema):
        cluster2path = {
            "primary": table_path,
            "remote0": f"{table_path}_0",
            "remote1": f"{table_path}_1",
            "remote2": f"{table_path}_2",
        }
        cluster2attrs = {
            "primary": {"dynamic": True},
            "remote0": {"dynamic": True, "my_attr0": "value0"},
            "remote1": {"dynamic": True, "my_attr1": "value1"},
            "remote2": {"dynamic": True, "my_attr2": "value2"},
        }
        cluster2patch = {
            "primary": {"main": True},
            "remote0": {"replicated_table_tracker_enabled": True, "preferred_sync": True},
            "remote1": {"replicated_table_tracker_enabled": True, "preferred_sync": False},
            "remote2": {},
        }
        cluster2log = {
            "remote0": {"path": f"{table_path}_0_custom_log", "attributes": {"tablet_count": 2}},
            "remote1": {"path": f"{table_path}_1_custom_log"},
        }

        def _cluster_spec(cluster: str) -> dict[str, Any]:
            base = {
                "path": cluster2path[cluster],
                "attributes": cluster2attrs[cluster],
            }
            base.update(cluster2patch[cluster])
            if cluster in cluster2log:
                base["replication_log"] = cluster2log[cluster]
            return base

        spec: TableSpec = TableSpec.parse(
            {
                "chaos": True,
                "in_collocation": True,
                "schema": default_schema,
                "clusters": {
                    "primary": _cluster_spec("primary"),
                    "remote0": _cluster_spec("remote0"),
                    "remote1": _cluster_spec("remote1"),
                    "remote2": _cluster_spec("remote2"),
                },
            }
        )

        assert spec.chaos
        assert spec.in_collocation
        assert spec.is_federation
        assert spec.schema == SchemaSpec.parse(default_schema)

        for cluster_name in cluster2path:
            assert cluster_name in spec.clusters
            cluster_spec = spec.clusters[cluster_name]
            if cluster_name == "primary":
                assert cluster_spec.main
                assert not cluster_spec.replicated_table_tracker_enabled
                assert not cluster_spec.preferred_sync
                assert cluster_spec.replication_log is None
            else:
                assert not cluster_spec.main
                if cluster_name in ("remote0", "remote1"):
                    assert cluster_spec.replicated_table_tracker_enabled
                else:
                    assert not cluster_spec.replicated_table_tracker_enabled
                if cluster_name == "remote0":
                    assert cluster_spec.preferred_sync
                else:
                    assert not cluster_spec.preferred_sync
                assert cluster_spec.replication_log is not None
                rlog_spec = cluster_spec.replication_log
                match cluster_name:
                    case "remote0":
                        assert rlog_spec.path == cluster2log["remote0"]["path"]
                        assert rlog_spec.attributes == cluster2log["remote0"]["attributes"]
                    case "remote1":
                        assert rlog_spec.path == cluster2log["remote1"]["path"]
                        assert rlog_spec.attributes == {}
                    case "remote2":
                        assert rlog_spec.path == TableSpec.make_replication_log_name(cluster_spec.path)
                        assert rlog_spec.attributes == {}
            assert cluster_spec.path == cluster2path[cluster_name]
            assert cluster_spec.attributes == cluster2attrs[cluster_name]
