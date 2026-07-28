"""Tests for the mini easy mode (``run_yt_sync_easy_mode`` shim).

Unit tests exercise spec/stage resolution and validation without a YT
cluster (every expected rejection fires before anything is created);
``TestEnsureOnLocalYt`` runs the real ``ensure`` against the recipe cluster
and checks the created Cypress objects.
"""

import enum
import os

import pytest

import yt.wrapper as yt

from yt.yt.flow.library.python.pipeline_tables.schemas import (
    PIPELINE_QUEUES,
    PIPELINE_TABLES,
)
from yt.yt.flow.library.python.yt_sync_mini import (
    QUEUE_META_COLUMNS,
    StagesSpec,
    run_yt_sync_easy_mode,
)

ENSURE_ARGS = ["--scenario", "ensure", "--parallel-factor", "0", "--commit"]

CLUSTER = "primary"


def make_stages(folder, cluster=CLUSTER):
    """The stereotype ``stages`` dict every flow test builds."""
    return {
        "default": {},
        "test": {
            "folder": folder,
            "presets": {
                "builtin:storage_preset": {"clusters": {cluster: {"attributes": {"primary_medium": "default"}}}},
                "builtin:table_preset": {"clusters": {cluster: {"attributes": {"tablet_cell_bundle": "default"}}}},
            },
        },
    }


def make_stages_spec(folder, cluster=CLUSTER):
    """Queue + sorted table + consumer + producer + pipeline, as flow tests do."""
    tables = {
        "state_table": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "key", "type": "string", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"},
                ],
            },
        },
        "input_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [{"name": "data", "type": "string"}] + QUEUE_META_COLUMNS,
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": 3}},
                },
            },
        },
    }
    consumers = {
        "consumer": {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {"input_queue": {"vital": True}},
            },
        },
    }
    producers = {
        "producer": {
            "default": {
                "$merge_presets": ["builtin:producer_preset"],
            },
        },
    }
    pipelines = {
        "pipeline": {
            "default": {
                "$merge_presets": ["builtin:pipeline_preset"],
                "monitoring_project": "",
                "monitoring_cluster": "",
            },
        },
    }
    return StagesSpec(
        stages=make_stages(folder, cluster),
        tables=tables,
        consumers=consumers,
        producers=producers,
        pipelines=pipelines,
    )


# ---------------------------------------------------------------------------
# Unit tests (no YT access: validation errors / entity-less specs).
# ---------------------------------------------------------------------------


def test_unknown_argument_rejected():
    with pytest.raises(NotImplementedError, match="--verbose"):
        run_yt_sync_easy_mode(
            "test",
            make_stages_spec("//tmp/never_created"),
            args=ENSURE_ARGS + ["--verbose"],
            exit_on_finish=False,
            setup_logging=False,
        )


def test_missing_commit_rejected():
    """Every flow invocation passes ``--commit``; yt_sync's dry-run default
    is out of the shim's scope and must fail loudly."""
    with pytest.raises(NotImplementedError, match="commit"):
        run_yt_sync_easy_mode(
            "test",
            make_stages_spec("//tmp/never_created"),
            args=["--scenario", "ensure", "--parallel-factor", "0"],
            exit_on_finish=False,
            setup_logging=False,
        )


def test_single_stage_is_implied():
    """With exactly one non-default stage, ``--stage`` may be omitted
    (yt_sync behaviour the test binaries rely on). An entity-less spec runs
    the full path without touching YT."""
    exit_code = run_yt_sync_easy_mode(
        "test",
        StagesSpec(stages=make_stages("//tmp/never_created")),
        args=ENSURE_ARGS,
        exit_on_finish=False,
        setup_logging=False,
    )
    assert exit_code == 0


def test_explicit_stage_flag_accepted():
    """The ``--stage test`` CLI form (used by yt_sync binaries and docker)."""
    exit_code = run_yt_sync_easy_mode(
        "test",
        StagesSpec(stages=make_stages("//tmp/never_created")),
        args=["--stage", "test"] + ENSURE_ARGS,
        exit_on_finish=False,
        setup_logging=False,
    )
    assert exit_code == 0


def test_unknown_scenario_rejected():
    with pytest.raises(NotImplementedError, match="scenario"):
        run_yt_sync_easy_mode(
            "test",
            make_stages_spec("//tmp/never_created"),
            args=["--scenario", "ensure_heavy", "--parallel-factor", "0", "--commit"],
            exit_on_finish=False,
            setup_logging=False,
        )


def test_setup_logging_rejected():
    """Logging setup is out of the shim's scope; asking for it must not be
    silently ignored."""
    with pytest.raises(NotImplementedError, match="setup_logging"):
        run_yt_sync_easy_mode(
            "test", make_stages_spec("//tmp/never_created"), args=[], exit_on_finish=False, setup_logging=True
        )


def test_nodes_rejected():
    spec = make_stages_spec("//tmp/never_created")
    spec.nodes = {"some_node": {"default": {}}}
    with pytest.raises(NotImplementedError, match="nodes"):
        run_yt_sync_easy_mode("test", spec, args=ENSURE_ARGS, exit_on_finish=False, setup_logging=False)


def test_unsupported_spec_key_rejected():
    """An entity-spec key outside the supported envelope fails loudly instead
    of being silently dropped (before anything is created)."""
    spec = make_stages_spec("//tmp/never_created")
    spec.tables["state_table"]["default"]["acl"] = []
    with pytest.raises(NotImplementedError, match="acl"):
        run_yt_sync_easy_mode("test", spec, args=ENSURE_ARGS, exit_on_finish=False, setup_logging=False)


def test_multiple_stages_rejected():
    """Every flow spec declares exactly one non-default stage."""
    spec = make_stages_spec("//tmp/never_created")
    spec.stages["prestable"] = spec.stages["test"]
    with pytest.raises(NotImplementedError, match="exactly one"):
        run_yt_sync_easy_mode("test", spec, args=ENSURE_ARGS, exit_on_finish=False, setup_logging=False)


def test_stage_without_cluster_rejected():
    """A stage whose presets pin no concrete cluster fails up front — the
    shim would not know which cluster to talk to."""
    spec = make_stages_spec("//tmp/never_created")
    spec.stages["test"]["presets"] = {}
    with pytest.raises(ValueError, match="concrete cluster"):
        run_yt_sync_easy_mode("test", spec, args=ENSURE_ARGS, exit_on_finish=False, setup_logging=False)


def test_unknown_preset_rejected():
    """An unknown preset name fails in the resolver before anything is
    created, instead of being silently skipped."""
    spec = make_stages_spec("//tmp/never_created")
    spec.tables["state_table"]["default"]["$merge_presets"] = ["my_preset"]
    with pytest.raises(KeyError, match="my_preset"):
        run_yt_sync_easy_mode("test", spec, args=ENSURE_ARGS, exit_on_finish=False, setup_logging=False)


def test_consumer_referencing_unknown_queue_rejected():
    """Registrations are validated before the consumer is created."""
    spec = StagesSpec(
        stages=make_stages("//tmp/never_created"),
        consumers={
            "consumer": {
                "default": {
                    "$merge_presets": ["builtin:consumer_preset"],
                    "in_stage_queues": {"no_such_queue": {"vital": True}},
                },
            },
        },
    )
    with pytest.raises(ValueError, match="no_such_queue"):
        run_yt_sync_easy_mode("test", spec, args=ENSURE_ARGS, exit_on_finish=False, setup_logging=False)


def test_per_stage_entity_overlay_rejected():
    """Entity specs must keep everything under ``"default"``; per-stage
    overlays (a yt_sync feature no flow spec uses) fail loudly before
    anything is created."""
    spec = make_stages_spec("//tmp/never_created")
    spec.tables["state_table"]["test"] = {"schema": []}
    with pytest.raises(NotImplementedError, match="state_table"):
        run_yt_sync_easy_mode("test", spec, args=ENSURE_ARGS, exit_on_finish=False, setup_logging=False)


def test_enum_stage_keys_accepted():
    """Stage-dict keys may be ``str``-enum members (as in the noop example's
    yt_sync binary): ``--stage test`` must match ``Stage.TEST``."""

    class Stage(str, enum.Enum):
        TEST = "test"

    spec = StagesSpec(stages={"default": {}, Stage.TEST: make_stages("//tmp/never_created")["test"]})
    exit_code = run_yt_sync_easy_mode(
        "test", spec, args=["--stage", "test"] + ENSURE_ARGS, exit_on_finish=False, setup_logging=False
    )
    assert exit_code == 0


# ---------------------------------------------------------------------------
# Integration: real ensure against the recipe cluster.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def yt_client():
    proxy = os.environ.get("YT_PROXY_PRIMARY") or os.environ.get("YT_PROXY")
    assert proxy, "YT_PROXY[_PRIMARY] env var must be set by the YT recipe"
    return yt.YtClient(proxy=proxy)


class TestEnsureOnLocalYt:
    FOLDER = "//tmp/yt_sync_mini_easy_mode"

    @pytest.fixture(scope="class", autouse=True)
    def bootstrap(self, request):
        proxy = os.environ.get("YT_PROXY_PRIMARY") or os.environ.get("YT_PROXY")
        client = yt.YtClient(proxy=proxy)
        if client.exists(self.FOLDER):
            client.remove(self.FOLDER, recursive=True, force=True)
        # The recipe registers cluster "primary" in YT_PROXY_URL_ALIASING_CONFIG,
        # which is how flow tests' cluster names resolve to proxy addresses.
        exit_code = run_yt_sync_easy_mode(
            "test",
            make_stages_spec(self.FOLDER),
            args=ENSURE_ARGS,
            exit_on_finish=False,
            setup_logging=False,
        )
        assert exit_code == 0

    def test_queue_created_and_mounted(self, yt_client):
        path = f"{self.FOLDER}/input_queue"
        assert yt_client.get(f"{path}/@dynamic")
        assert yt_client.get(f"{path}/@tablet_count") == 3
        assert yt_client.get(f"{path}/@tablet_state") == "mounted"
        schema = yt_client.get(f"{path}/@schema")
        assert not schema.attributes["unique_keys"]

    def test_sorted_table_created_and_mounted(self, yt_client):
        path = f"{self.FOLDER}/state_table"
        assert yt_client.get(f"{path}/@dynamic")
        assert yt_client.get(f"{path}/@tablet_state") == "mounted"
        schema = yt_client.get(f"{path}/@schema")
        assert schema.attributes["unique_keys"]
        assert yt_client.get(f"{path}/@tablet_cell_bundle") == "default"

    def test_consumer_created_and_registered(self, yt_client):
        path = f"{self.FOLDER}/consumer"
        assert yt_client.get(f"{path}/@treat_as_queue_consumer")
        assert yt_client.get(f"{path}/@tablet_state") == "mounted"
        registrations = list(
            yt_client.list_queue_consumer_registrations(queue_path=f"{self.FOLDER}/input_queue", consumer_path=path)
        )
        assert len(registrations) == 1
        assert registrations[0]["vital"]

    def test_producer_created(self, yt_client):
        path = f"{self.FOLDER}/producer"
        assert yt_client.get(f"{path}/@treat_as_queue_producer")
        assert yt_client.get(f"{path}/@tablet_state") == "mounted"

    def test_pipeline_created(self, yt_client):
        path = f"{self.FOLDER}/pipeline"
        assert yt_client.get(f"{path}/@pipeline_format_version") == 1
        for name in list(PIPELINE_TABLES) + list(PIPELINE_QUEUES):
            assert yt_client.get(f"{path}/{name}/@tablet_state") == "mounted", name

    def test_rerun_is_idempotent(self, yt_client):
        exit_code = run_yt_sync_easy_mode(
            "test",
            make_stages_spec(self.FOLDER),
            args=ENSURE_ARGS,
            exit_on_finish=False,
            setup_logging=False,
        )
        assert exit_code == 0
        assert yt_client.get(f"{self.FOLDER}/input_queue/@tablet_state") == "mounted"
