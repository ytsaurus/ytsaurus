import io
import json

import pytest
from yt.yson import YsonBoolean

from yt.yt.tools.pod_size_actualization.optimization.instance_sizes import (
    build_cluster_instance_sizes,
    collect_group_instance_sizes,
    collect_group_instance_sizes_with_fallback,
    ensure_cluster_instance_sizes_match,
)


def size(vcpu, memory_gib, network_mib, **extra):
    return {
        "resource_guarantee": {
            "vcpu": vcpu,
            "memory": memory_gib * 2**30,
            "net_bytes": network_mib * 2**20,
        },
        **extra,
    }


def test_build_catalog_filters_only_explicitly_deprecated_sizes():
    rows = build_cluster_instance_sizes(
        {
            "medium": size(14000, 100, 320),
            "tiny": size(4000, 20, 12.5, deprecated=False),
            "old": size(2000, 10, 5, deprecated=True, deprecation_reason="use tiny"),
        },
        {"medium": size(8000, 20, 130)},
        cluster="pythia",
    )

    assert rows == [
        {
            "ContainerTypeID": 0,
            "InstanceType": "node",
            "ConfigName": "tiny",
            "CPU": 4.0,
            "Memory": 20.0,
            "Network": 12.5,
        },
        {
            "ContainerTypeID": 1,
            "InstanceType": "node",
            "ConfigName": "medium",
            "CPU": 14.0,
            "Memory": 100.0,
            "Network": 320.0,
        },
        {
            "ContainerTypeID": 2,
            "InstanceType": "proxy",
            "ConfigName": "medium",
            "CPU": 8.0,
            "Memory": 20.0,
            "Network": 130.0,
        },
    ]


def test_absent_deprecated_flag_is_allowed():
    rows = build_cluster_instance_sizes(
        {"tiny": size(4000, 20, 10)},
        {},
        cluster="seneca-sas",
    )
    assert [row["ConfigName"] for row in rows] == ["tiny"]


def test_yson_boolean_deprecated_is_supported():
    rows = build_cluster_instance_sizes(
        {
            "tiny": size(4000, 20, 10, deprecated=YsonBoolean(False)),
            "old": size(2000, 10, 5, deprecated=YsonBoolean(True)),
        },
        {},
        cluster="pythia",
    )
    assert [row["ConfigName"] for row in rows] == ["tiny"]


def test_group_catalogs_are_compared_after_deprecated_filtering():
    common = build_cluster_instance_sizes(
        {"tiny": size(4000, 20, 10)},
        {},
        cluster="seneca-sas",
    )
    with_deprecated_filtered_out = build_cluster_instance_sizes(
        {
            "tiny": size(4000, 20, 10),
            "old": size(2000, 10, 5, deprecated=True),
        },
        {},
        cluster="seneca-vla",
    )

    assert (
        ensure_cluster_instance_sizes_match(
            {
                "seneca-sas": common,
                "seneca-vla": with_deprecated_filtered_out,
            }
        )
        == common
    )


def test_group_catalog_mismatch_fails_with_changed_size():
    sas = build_cluster_instance_sizes(
        {"tiny": size(4000, 20, 10)},
        {},
        cluster="seneca-sas",
    )
    vla = build_cluster_instance_sizes(
        {"tiny": size(5000, 20, 10)},
        {},
        cluster="seneca-vla",
    )

    with pytest.raises(ValueError, match=r"changed=\[\('node', 'tiny'\)\]"):
        ensure_cluster_instance_sizes_match({"seneca-sas": sas, "seneca-vla": vla})


def test_missing_size_does_not_report_shifted_ids_as_changed():
    sas = build_cluster_instance_sizes(
        {"tiny": size(4000, 20, 10), "medium": size(14000, 100, 320)},
        {},
        cluster="seneca-sas",
    )
    vla = build_cluster_instance_sizes(
        {"medium": size(14000, 100, 320)},
        {},
        cluster="seneca-vla",
    )

    with pytest.raises(ValueError, match=r"missing=\[\('node', 'tiny'\)\].*changed=\[\]"):
        ensure_cluster_instance_sizes_match({"seneca-sas": sas, "seneca-vla": vla})


def test_deprecated_must_be_boolean_when_present():
    with pytest.raises(TypeError, match="deprecated must be boolean"):
        build_cluster_instance_sizes(
            {"tiny": size(4000, 20, 10, deprecated="false")},
            {},
            cluster="pythia",
        )


def test_collect_single_cluster_group_reads_only_its_cluster():
    requested = []

    class Client:
        def __init__(self, cluster):
            self.cluster = cluster

        def get(self, path):
            requested.append((self.cluster, path))
            if path.endswith("/@tablet_node_sizes"):
                return {"tiny": size(4000, 20, 10)}
            return {"medium": size(8000, 20, 130)}

    rows = collect_group_instance_sizes("hahn", Client)

    assert [row["InstanceType"] for row in rows] == ["node", "proxy"]
    assert requested == [
        ("hahn", "//sys/bundle_controller/controller/zones/zone_default/@tablet_node_sizes"),
        ("hahn", "//sys/bundle_controller/controller/zones/zone_default/@rpc_proxy_sizes"),
    ]


def test_collect_unknown_cluster_group_fails_before_creating_clients():
    with pytest.raises(ValueError, match="unknown cluster group 'unknown'"):
        collect_group_instance_sizes("unknown", lambda cluster: pytest.fail(cluster))


class CacheClient:
    def __init__(self, payload=None):
        self.payload = None if payload is None else json.dumps(payload).encode()
        self.created = []
        self.set_calls = []
        self.read_count = 0

    def create(self, node_type, path, **kwargs):
        self.created.append((node_type, path, kwargs))

    def write_file(self, path, payload):
        self.payload = payload

    def set(self, path, value):
        self.set_calls.append((path, value))

    def read_file(self, path):
        self.read_count += 1
        return io.BytesIO(self.payload)


def test_successful_group_read_updates_cache_with_loaded_at():
    class Client:
        def get(self, path):
            if path.endswith("/@tablet_node_sizes"):
                return {"tiny": size(4000, 20, 10)}
            return {"medium": size(8000, 20, 130)}

    cache = CacheClient()
    payload = collect_group_instance_sizes_with_fallback(
        "hahn",
        lambda cluster: Client(),
        cache,
        "//tmp/instance_sizes/hahn.json",
        loaded_at="2026-08-13T10:00:00+00:00",
    )

    assert payload["cluster_group_catalog_loaded_at"] == "2026-08-13T10:00:00+00:00"
    assert json.loads(cache.payload) == payload
    assert cache.created == [
        (
            "file",
            "//tmp/instance_sizes/hahn.json",
            {
                "recursive": True,
                "ignore_existing": True,
                "attributes": {"account": "tmp"},
            },
        )
    ]
    assert cache.set_calls == [("//tmp/instance_sizes/hahn.json/@account", "tmp")]
    assert cache.read_count == 0


def test_failed_get_uses_cached_group_catalog():
    cached = {
        "sizes": [{"InstanceType": "node", "ConfigName": "cached"}],
        "cluster_group_catalog_loaded_at": "2026-08-12T10:00:00+00:00",
    }

    class Client:
        def get(self, path):
            raise RuntimeError("cluster unavailable")

    cache = CacheClient(cached)
    assert (
        collect_group_instance_sizes_with_fallback(
            "hahn",
            lambda cluster: Client(),
            cache,
            "//tmp/instance_sizes/hahn.json",
        )
        == cached
    )
    assert cache.read_count == 1
    assert cache.created == []


def test_force_cluster_read_does_not_use_cache_after_failed_get():
    class Client:
        def get(self, path):
            raise RuntimeError("cluster unavailable")

    cache = CacheClient(
        {
            "sizes": [],
            "cluster_group_catalog_loaded_at": "2026-08-12T10:00:00+00:00",
        }
    )
    with pytest.raises(RuntimeError, match="cluster unavailable"):
        collect_group_instance_sizes_with_fallback(
            "hahn",
            lambda cluster: Client(),
            cache,
            "//tmp/instance_sizes/hahn.json",
            force_cluster_read=True,
        )
    assert cache.read_count == 0


def test_catalog_validation_error_is_not_hidden_by_cache():
    class Client:
        def get(self, path):
            if path.endswith("/@tablet_node_sizes"):
                return {"broken": {}}
            return {}

    cache = CacheClient(
        {
            "sizes": [{"InstanceType": "node", "ConfigName": "cached"}],
            "cluster_group_catalog_loaded_at": "2026-08-12T10:00:00+00:00",
        }
    )
    with pytest.raises(ValueError, match="has no resource_guarantee map"):
        collect_group_instance_sizes_with_fallback(
            "hahn",
            lambda cluster: Client(),
            cache,
            "//tmp/instance_sizes/hahn.json",
        )
    assert cache.read_count == 0
