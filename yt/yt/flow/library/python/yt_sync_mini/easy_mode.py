"""Mini replacement for yt_sync's easy mode."""

import copy
import dataclasses
import sys

import yt.wrapper as yt

from .yt_sync_mini import (
    CONSUMER_SCHEMA,
    LOCAL_PRESETS,
    PRESET_MERGE_KEY,
    PRODUCER_SCHEMA,
    _build_schema,
    _deep_merge,
    create_pipeline,
    create_table,
    register_consumer,
)


@dataclasses.dataclass
class StagesSpec:
    stages: dict = dataclasses.field(default_factory=dict)
    tables: dict = dataclasses.field(default_factory=dict)
    consumers: dict = dataclasses.field(default_factory=dict)
    producers: dict = dataclasses.field(default_factory=dict)
    nodes: dict = dataclasses.field(default_factory=dict)
    pipelines: dict = dataclasses.field(default_factory=dict)


def run_yt_sync_easy_mode(
    name,
    stages_spec,
    args=None,
    exit_on_finish=False,
    setup_logging=False,
):
    if setup_logging:
        raise NotImplementedError("setup_logging is not supported by yt_sync_mini")
    if stages_spec.nodes:
        raise NotImplementedError("nodes are not supported by yt_sync_mini")

    args = list(sys.argv[1:] if args is None else args)
    stage = _select_stage(stages_spec.stages, args)
    _check_args(stage, args)
    _ensure_stage(stages_spec, stage)
    return 0


_EXPECTED_ARGS = ["--scenario", "ensure", "--parallel-factor", "0", "--commit"]


def _stage_name(stage):
    return str(stage.value if hasattr(stage, "value") else stage)


def _select_stage(stages, args):
    stage_keys = [stage for stage in stages if _stage_name(stage) != "default"]
    requested = None
    if "--stage" in args:
        index = args.index("--stage")
        if index + 1 >= len(args):
            raise ValueError("--stage requires a value")
        requested = args[index + 1]

    if requested is not None:
        matches = [stage for stage in stage_keys if _stage_name(stage) == requested]
        if not matches:
            raise ValueError(f"unknown stage {requested!r}")
        return matches[0]

    if len(stage_keys) != 1:
        raise NotImplementedError(
            f"exactly one non-default stage or an explicit --stage is required, got {sorted(map(_stage_name, stage_keys))}"
        )
    return stage_keys[0]


def _check_args(stage, args):
    tokens = list(args)
    if tokens[:2] == ["--stage", _stage_name(stage)]:
        tokens = tokens[2:]
    if tokens != _EXPECTED_ARGS:
        raise NotImplementedError(
            f"yt_sync_mini supports only ['--stage', {_stage_name(stage)!r}] + {_EXPECTED_ARGS}, got {list(args)}"
        )


def _check_keys(kind, name, spec, allowed):
    unknown = sorted(set(spec) - set(allowed), key=str)
    if unknown:
        raise NotImplementedError(f"{kind} {name!r}: spec keys {unknown} are not supported by yt_sync_mini")


def _overlay(base, patch):
    result = copy.deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _overlay(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _stage_spec(stages, stage):
    default = stages.get("default", {})
    selected = stages[stage]
    _check_keys("stage", stage, default, {"folder", "presets"})
    _check_keys("stage", stage, selected, {"folder", "presets"})
    return _overlay(default, selected)


def _preset_registry(stages, stage):
    registry = copy.deepcopy(LOCAL_PRESETS)
    for stage_key in ("default", stage):
        for name, preset in stages.get(stage_key, {}).get("presets", {}).items():
            registry[name] = _overlay(registry.get(name, {}), preset)
    return registry


def _cluster_overlays(registry):
    result = {}
    for preset in registry.values():
        for cluster, overlay in preset.get("clusters", {}).items():
            if not str(cluster).startswith("_"):
                result[cluster] = _overlay(result.get(cluster, {}), overlay)
    return result


def _resolve_cluster_attributes(spec, registry, cluster):
    merged = {}
    for parent_name in spec.get(PRESET_MERGE_KEY, []):
        if parent_name not in registry:
            raise KeyError(f"unknown preset {parent_name!r} referenced via {PRESET_MERGE_KEY!r}")
        _deep_merge(merged, _resolve_cluster_attributes(registry[parent_name], registry, cluster))

    for selector, overlay in spec.get("clusters", {}).items():
        if selector in (cluster, "_all_clusters", "_all_data_clusters"):
            attrs = overlay.get("attributes")
            if attrs:
                _deep_merge(merged, attrs)
    return merged


def _collect_clusters(spec, registry, all_clusters, seen=None):
    seen = set() if seen is None else seen
    clusters = set()
    for parent_name in spec.get(PRESET_MERGE_KEY, []):
        if parent_name in seen:
            continue
        if parent_name not in registry:
            raise KeyError(f"unknown preset {parent_name!r} referenced via {PRESET_MERGE_KEY!r}")
        seen.add(parent_name)
        clusters.update(_collect_clusters(registry[parent_name], registry, all_clusters, seen))
    for selector in spec.get("clusters", {}):
        if str(selector).startswith("_"):
            clusters.update(all_clusters)
        else:
            clusters.add(selector)
    return clusters


def _entity_spec(kind, name, entity_spec, stages, stage):
    allowed = {"default", *stages}
    _check_keys(kind, name, entity_spec, allowed)
    return _overlay(entity_spec.get("default", {}), entity_spec.get(stage, {}))


class _Ensurer:
    def __init__(self, folder, main_cluster, clusters, registry):
        self._folder = folder
        self._main_cluster = main_cluster
        self._clusters = clusters
        self._registry = registry
        self._clients = {}

    def _get_client(self, cluster=None):
        cluster = cluster or self._main_cluster
        if cluster not in self._clients:
            self._clients[cluster] = yt.YtClient(proxy=cluster, config=yt.default_config.get_config_from_env())
        return self._clients[cluster]

    def _target_clusters(self, spec):
        clusters = _collect_clusters(spec, self._registry, self._clusters)
        return clusters or {self._main_cluster}

    def _ensure_table(self, name, schema, spec):
        target_clusters = self._target_clusters(spec)
        if len(target_clusters) == 1:
            cluster = next(iter(target_clusters))
            attrs = _resolve_cluster_attributes(spec, self._registry, cluster)
            create_table(self._get_client(cluster), f"{self._folder}/{name}", schema, attrs)
            return

        if self._main_cluster not in target_clusters:
            raise ValueError(f"replicated table {name!r} does not include main cluster {self._main_cluster!r}")

        path = f"{self._folder}/{name}"
        replica_clusters = sorted(target_clusters - {self._main_cluster})
        for cluster in replica_clusters:
            attrs = _resolve_cluster_attributes(spec, self._registry, cluster)
            attrs.pop("replicated_table_options", None)
            create_table(self._get_client(cluster), path, schema, attrs)

        main_attrs = {"dynamic": True}
        _deep_merge(main_attrs, _resolve_cluster_attributes(spec, self._registry, self._main_cluster))
        main_attrs["schema"] = _build_schema(schema)
        rtt_options = main_attrs.setdefault("replicated_table_options", {})
        preferred_sync_clusters = [
            cluster for cluster in replica_clusters if self._clusters.get(cluster, {}).get("preferred_sync")
        ]
        if preferred_sync_clusters:
            rtt_options["preferred_sync_replica_clusters"] = preferred_sync_clusters

        client = self._get_client()
        client.create("replicated_table", path, recursive=True, ignore_existing=True, attributes=main_attrs)
        existing = client.get(f"{path}/@replicas")
        existing_keys = {(attrs["cluster_name"], attrs["replica_path"]) for attrs in existing.values()}
        for cluster in replica_clusters:
            if (cluster, path) in existing_keys:
                continue
            cluster_spec = self._clusters.get(cluster, {})
            client.create(
                "table_replica",
                attributes={
                    "table_path": path,
                    "cluster_name": cluster,
                    "replica_path": path,
                    "mode": "sync" if cluster_spec.get("preferred_sync") else "async",
                    "enabled": True,
                    "enable_replicated_table_tracker": bool(cluster_spec.get("replicated_table_tracker_enabled")),
                },
            )

    def ensure_table(self, name, spec):
        _check_keys("table", name, spec, {PRESET_MERGE_KEY, "schema", "clusters"})
        if "schema" not in spec:
            raise ValueError(f"table {name!r} defines no schema")
        self._ensure_table(name, spec["schema"], spec)

    def ensure_consumer(self, name, spec, known_queues):
        _check_keys("consumer", name, spec, {PRESET_MERGE_KEY, "in_stage_queues", "clusters"})
        registrations = spec.get("in_stage_queues", {})
        for queue_name, registration in registrations.items():
            _check_keys("queue registration", queue_name, registration, {"vital"})
            if queue_name not in known_queues:
                raise ValueError(f"consumer {name!r} references unknown in-stage queue {queue_name!r}")

        self._ensure_table(name, CONSUMER_SCHEMA, spec)
        client = self._get_client()
        consumer_path = f"{self._folder}/{name}"
        for queue_name, registration in registrations.items():
            register_consumer(
                client,
                f"{self._folder}/{queue_name}",
                consumer_path,
                bool(registration.get("vital", False)),
            )

    def ensure_producer(self, name, spec):
        _check_keys("producer", name, spec, {PRESET_MERGE_KEY, "clusters"})
        self._ensure_table(name, PRODUCER_SCHEMA, spec)

    def ensure_pipeline(self, name, spec):
        _check_keys("pipeline", name, spec, {PRESET_MERGE_KEY, "monitoring_project", "monitoring_cluster"})
        create_pipeline(self._get_client(), f"{self._folder}/{name}")


def _ensure_stage(stages_spec, stage):
    stage_spec = _stage_spec(stages_spec.stages, stage)
    folder = stage_spec.get("folder")
    if not folder:
        raise ValueError(f"stage {stage!r} must define 'folder'")

    registry = _preset_registry(stages_spec.stages, stage)
    clusters = _cluster_overlays(registry)
    main_clusters = [cluster for cluster, spec in clusters.items() if spec.get("main")]
    if len(main_clusters) == 1:
        main_cluster = main_clusters[0]
    elif len(clusters) == 1:
        main_cluster = next(iter(clusters))
    elif not clusters:
        raise ValueError("stage presets do not pin a concrete cluster")
    else:
        raise ValueError(f"stage presets must identify exactly one main cluster, got {sorted(main_clusters)}")

    ensurer = _Ensurer(folder, main_cluster, clusters, registry)
    for kind, ensure in (
        ("tables", ensurer.ensure_table),
        ("consumers", lambda name, spec: ensurer.ensure_consumer(name, spec, stages_spec.tables)),
        ("producers", ensurer.ensure_producer),
        ("pipelines", ensurer.ensure_pipeline),
    ):
        for name, entity_spec in getattr(stages_spec, kind).items():
            ensure(name, _entity_spec(kind, name, entity_spec, stages_spec.stages, stage))
