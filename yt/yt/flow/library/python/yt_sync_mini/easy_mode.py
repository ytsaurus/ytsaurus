"""Mini replacement for yt_sync's easy mode.

Supports exactly the spec shape flow tests and example bootstrappers use and
raises ``NotImplementedError`` for anything else.

"""

import dataclasses

import yt.wrapper as yt

from .yt_sync_mini import (
    CONSUMER_SCHEMA,
    LOCAL_PRESETS,
    PRESET_MERGE_KEY,
    PRODUCER_SCHEMA,
    _resolve_attributes,
    create_pipeline,
    create_table,
    register_consumer,
)


@dataclasses.dataclass
class StagesSpec:
    """Declarative Cypress spec, mirroring ``yt.yt_sync.runner.StagesSpec``.

    Entity dicts are keyed ``{entity_name: {"default": spec}}`` — unlike
    yt_sync, per-stage entity overlays are not supported.
    """

    stages: dict = dataclasses.field(default_factory=dict)
    tables: dict = dataclasses.field(default_factory=dict)
    consumers: dict = dataclasses.field(default_factory=dict)
    producers: dict = dataclasses.field(default_factory=dict)
    nodes: dict = dataclasses.field(default_factory=dict)
    pipelines: dict = dataclasses.field(default_factory=dict)


def run_yt_sync_easy_mode(
    name,
    stages_spec,
    args,
    exit_on_finish=False,
    setup_logging=False,
):
    """Entry point mirroring ``yt.yt_sync.runner.run_yt_sync_easy_mode``."""
    if setup_logging:
        raise NotImplementedError("setup_logging is not supported by yt_sync_mini")
    if stages_spec.nodes:
        raise NotImplementedError("nodes are not supported by yt_sync_mini")

    stage_names = set(stages_spec.stages) - {"default"}
    if len(stage_names) != 1:
        raise NotImplementedError(
            f"exactly one non-default stage is supported by yt_sync_mini, got {sorted(map(str, stage_names))}"
        )
    (stage,) = stage_names

    _check_args(stage, args)
    _ensure_stage(stages_spec, stage)
    return 0


_EXPECTED_ARGS = ["--scenario", "ensure", "--parallel-factor", "0", "--commit"]


def _check_args(stage, args):
    """Validate the fixed argument list: an optional ``--stage <stage>``
    prefix followed by exactly :data:`_EXPECTED_ARGS`."""
    tokens = list(args)
    if tokens[:2] == ["--stage", stage]:
        tokens = tokens[2:]
    if tokens != _EXPECTED_ARGS:
        raise NotImplementedError(
            f"yt_sync_mini supports only ['--stage', {str(stage)!r}] + {_EXPECTED_ARGS}, got {list(args)}"
        )


def _check_keys(kind, name, spec, allowed):
    unknown = sorted(set(spec) - allowed)
    if unknown:
        raise NotImplementedError(f"{kind} {name!r}: spec keys {unknown} are not supported by yt_sync_mini")


def _stage_cluster(stages, stage):
    """Extract the target ``(cluster, folder)`` from the stage spec: the
    folder key and the single concrete cluster named by the presets."""
    stage_spec = stages[stage]
    _check_keys("stage", stage, stage_spec, {"folder", "presets"})

    folder = stage_spec.get("folder")
    if not folder:
        raise ValueError(f"stage {stage!r} must define 'folder'")

    clusters = {
        cluster
        for preset in stage_spec.get("presets", {}).values()
        for cluster in preset.get("clusters", {})
        if not cluster.startswith("_")
    }
    if len(clusters) > 1:
        raise NotImplementedError(f"stage presets must pin a single cluster, got {sorted(clusters)}")
    if not clusters:
        raise ValueError("stage presets do not pin a concrete cluster")
    return clusters.pop(), folder


class _Ensurer:
    """Creates the entities of one stage on its (single) target cluster."""

    def __init__(self, folder, cluster):
        self._folder = folder
        self._cluster = cluster
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = yt.YtClient(proxy=self._cluster, config=yt.default_config.get_config_from_env())
        return self._client

    def ensure_table(self, name, spec):
        _check_keys("table", name, spec, {PRESET_MERGE_KEY, "schema", "clusters"})
        if "schema" not in spec:
            raise ValueError(f"table {name!r} defines no schema")
        attrs = _resolve_attributes(spec, LOCAL_PRESETS)
        create_table(self._get_client(), f"{self._folder}/{name}", spec["schema"], attrs)

    def ensure_consumer(self, name, spec, known_queues):
        _check_keys("consumer", name, spec, {PRESET_MERGE_KEY, "in_stage_queues"})
        registrations = spec.get("in_stage_queues", {})
        for queue_name, registration in registrations.items():
            _check_keys("queue registration", queue_name, registration, {"vital"})
            if queue_name not in known_queues:
                raise ValueError(f"consumer {name!r} references unknown in-stage queue {queue_name!r}")

        consumer_path = f"{self._folder}/{name}"
        create_table(self._get_client(), consumer_path, CONSUMER_SCHEMA, _resolve_attributes(spec, LOCAL_PRESETS))
        for queue_name, registration in registrations.items():
            queue_path = f"{self._folder}/{queue_name}"
            register_consumer(self._get_client(), queue_path, consumer_path, bool(registration.get("vital", False)))

    def ensure_producer(self, name, spec):
        _check_keys("producer", name, spec, {PRESET_MERGE_KEY})
        create_table(
            self._get_client(), f"{self._folder}/{name}", PRODUCER_SCHEMA, _resolve_attributes(spec, LOCAL_PRESETS)
        )

    def ensure_pipeline(self, name, spec):
        _check_keys("pipeline", name, spec, {PRESET_MERGE_KEY, "monitoring_project", "monitoring_cluster"})
        create_pipeline(self._get_client(), f"{self._folder}/{name}")


def _ensure_stage(stages_spec, stage):
    cluster, folder = _stage_cluster(stages_spec.stages, stage)
    ensurer = _Ensurer(folder, cluster)

    for kind, ensure in (
        ("tables", ensurer.ensure_table),
        ("consumers", lambda name, spec: ensurer.ensure_consumer(name, spec, stages_spec.tables)),
        ("producers", ensurer.ensure_producer),
        ("pipelines", ensurer.ensure_pipeline),
    ):
        for name, entity_spec in getattr(stages_spec, kind).items():
            # Per-stage entity overlays are not supported: everything lives under the "default" sub-spec.
            _check_keys(kind, name, entity_spec, {"default"})
            ensure(name, entity_spec.get("default", {}))
