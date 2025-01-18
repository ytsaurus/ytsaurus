from copy import deepcopy
from typing import Callable

import pytest

from yt.yt_sync.core.constants import CONSUMER_ATTRS
from yt.yt_sync.core.constants import CONSUMER_SCHEMA
from yt.yt_sync.core.constants import PIPELINE_FILES
from yt.yt_sync.core.constants import PIPELINE_QUEUES
from yt.yt_sync.core.constants import PIPELINE_TABLES
from yt.yt_sync.core.constants import PRODUCER_ATTRS
from yt.yt_sync.core.constants import PRODUCER_SCHEMA
from yt.yt_sync.core.spec import ClusterNode
from yt.yt_sync.core.spec import ClusterTable
from yt.yt_sync.core.spec import Consumer
from yt.yt_sync.core.spec import Node
from yt.yt_sync.core.spec import PipelineSpec
from yt.yt_sync.core.spec import Producer
from yt.yt_sync.core.spec import QueueRegistration
from yt.yt_sync.core.spec import SchemaSpec
from yt.yt_sync.core.spec import Table
from yt.yt_sync.runner.core import Description
from yt.yt_sync.runner.core import StageDescription
from yt.yt_sync.runner.core.description import StageEntityType
from yt.yt_sync.runner.core.details import DescriptionDetails


def _make_table(path: str) -> Table:
    return Table(
        schema=SchemaSpec.parse(
            [{"name": "K", "sort_order": "ascending", "type": "uint64"}, {"name": "V", "type": "uint64"}]
        ),
        clusters={"primary": ClusterTable(path=path)},
    )


def _make_consumer(path: str) -> Consumer:
    table = Table(
        schema=SchemaSpec.parse(CONSUMER_SCHEMA),
        clusters={"primary": ClusterTable(path=path, attributes=CONSUMER_ATTRS)},
    )
    return Consumer(table=table, queues=[QueueRegistration(cluster="primary", path=path)])


def _make_producer(path: str) -> Producer:
    table = Table(
        schema=SchemaSpec.parse(PRODUCER_SCHEMA),
        clusters={"primary": ClusterTable(path=path, attributes=PRODUCER_ATTRS)},
    )
    return Producer(table=table)


def _make_node(path: str) -> Node:
    return Node(type=Node.Type.FOLDER, clusters={"primary": ClusterNode(path=path)})


def _make_pipeline(path: str) -> Producer:
    common_spec = {
        "clusters": {
            "primary": {
                "attributes": {
                    "compression_codec": "lz4",
                    "tablet_cell_bundle": "test_bundle",
                    "primary_medium": "ssd_blobs",
                },
            },
        },
    }
    spec = {
        "path": path,
        "monitoring_project": "test-project",
        "monitoring_cluster": "test-cluster",
        "table_spec": {name: common_spec for name in PIPELINE_TABLES},
        "queue_spec": {name: common_spec for name in PIPELINE_QUEUES},
        "file_spec": {name: common_spec for name in PIPELINE_FILES},
    }
    return PipelineSpec.parse(spec).spec


def _make_entities_description(entity_type: StageEntityType, path: str) -> Description:
    make_entity = globals()[f"_make_{entity_type.value}"]
    entity_key = f"{entity_type.value}s"
    return Description(
        stages={
            "stage1": StageDescription(
                **{
                    entity_key: {
                        f"{entity_type.value}1": make_entity(path),
                        f"{entity_type.value}2": make_entity(path),
                    }
                },
            ),
            "stage2": StageDescription(
                **{
                    entity_key: {
                        f"{entity_type.value}2": make_entity(path),
                        f"{entity_type.value}3": make_entity(path),
                    }
                },
            ),
        }
    )


def test_get_all_stages():
    description = Description(stages={"stage1": StageDescription(), "stage2": StageDescription()})
    details = DescriptionDetails(description)
    assert details.get_all_stages() == ["stage1", "stage2"]


@pytest.mark.parametrize("entity_type", list(StageEntityType))
def test_get_all(entity_type: StageEntityType, table_path: str):
    description = _make_entities_description(entity_type, path=table_path)
    details = DescriptionDetails(description)
    assert details.get_all(entity_type) == [f"{entity_type.value}{i}" for i in [1, 2, 3]]


@pytest.mark.parametrize("entity_type", list(StageEntityType))
def test_validate_filter_errors(entity_type: StageEntityType, table_path: str):
    def _test_bad_stage(v: Callable[[str], None]):
        with pytest.raises(RuntimeError):
            v("unknown")

    def _test_bad_special(v: Callable[[list[str]], None]):
        with pytest.raises(RuntimeError):
            v(["all", "none"])

    def _test_not_single_special(v: Callable[[str], None]):
        with pytest.raises(RuntimeError):
            v("all")

        with pytest.raises(RuntimeError):
            v("none")

    def _test_unknown_item(v: Callable[[], None]):
        with pytest.raises(RuntimeError):
            v()

    details = DescriptionDetails(_make_entities_description(entity_type, path=table_path))
    _test_bad_stage(lambda stage: details.validate_filter(entity_type, stage, []))
    _test_bad_special(lambda filters: details.validate_filter(entity_type, "stage1", filters))
    _test_not_single_special(lambda s: details.validate_filter(entity_type, "stage1", [s, f"{entity_type.value}1"]))
    _test_unknown_item(lambda: details.validate_filter(entity_type, "stage1", "X"))


@pytest.mark.parametrize("entity_type", list(StageEntityType))
def test_validate_filter(entity_type: StageEntityType, table_path: str):
    details = DescriptionDetails(_make_entities_description(entity_type, path=table_path))

    details.validate_filter(entity_type, "stage1", ["all"])
    details.validate_filter(entity_type, "stage1", ["none"])
    details.validate_filter(entity_type, "stage1", [f"{entity_type.value}1", f"{entity_type.value}2"])

    details.validate_filter(entity_type, "stage2", ["all"])
    details.validate_filter(entity_type, "stage2", ["none"])
    details.validate_filter(entity_type, "stage2", [f"{entity_type.value}2", f"{entity_type.value}3"])


@pytest.mark.parametrize("entity_type", list(StageEntityType))
def test_get_prepared_stage_descriptions(entity_type: StageEntityType, table_path: str):
    details = DescriptionDetails(_make_entities_description(entity_type, table_path))

    def entities(*given_names):
        return {entity_type: {*given_names}}

    def get_stage_entities_keys(stage):
        return getattr(stage, f"{entity_type.value}s").keys()

    # unknown stage
    with pytest.raises(RuntimeError):
        details.get_prepared_stage_descriptions("unknown", entities())

    # all
    stage1_all = details.get_prepared_stage_descriptions("stage1", entities("all"))
    assert len(stage1_all) == 1
    assert get_stage_entities_keys(stage1_all[0]) == {f"{entity_type.value}1", f"{entity_type.value}2"}

    stage2_all = details.get_prepared_stage_descriptions("stage2", entities("all"))
    assert len(stage2_all) == 1
    assert get_stage_entities_keys(stage2_all[0]) == {f"{entity_type.value}2", f"{entity_type.value}3"}

    # none
    stage1_none = details.get_prepared_stage_descriptions("stage1", entities("none"))
    assert len(stage1_none) == 0

    stage2_none = details.get_prepared_stage_descriptions("stage2", entities("none"))
    assert len(stage2_none) == 0

    # filter
    stage1_flt = details.get_prepared_stage_descriptions("stage1", entities(f"{entity_type.value}1"))
    assert len(stage1_flt) == 1
    assert get_stage_entities_keys(stage1_flt[0]) == {f"{entity_type.value}1"}

    stage2_flt = details.get_prepared_stage_descriptions("stage2", entities(f"{entity_type.value}3"))
    assert len(stage2_flt) == 1
    assert get_stage_entities_keys(stage2_flt[0]) == {f"{entity_type.value}3"}


@pytest.mark.parametrize("custom_locks", [[], ["//tmp/custom/lock"]])
def test_get_prepared_stage_descriptions_split_main(table_path: str, custom_locks: list[str]):
    description = _make_entities_description(StageEntityType.TABLE, table_path)
    consumer_description = _make_entities_description(StageEntityType.CONSUMER, table_path)
    node_description = _make_entities_description(StageEntityType.NODE, table_path)

    # merge descriptions to one
    for stage in ("stage1", "stage2"):
        description.stages[stage].consumers = deepcopy(consumer_description.stages[stage].consumers)
        description.stages[stage].nodes = deepcopy(node_description.stages[stage].nodes)
        description.stages[stage].locks = deepcopy(custom_locks)

    # patch main cluster for one item
    stage1 = description.stages["stage1"]

    table2_clusters = stage1.tables["table2"].clusters
    table2_clusters["secondary"] = table2_clusters["primary"]
    table2_clusters.pop("primary")

    consumer2_clusters = stage1.consumers["consumer2"].table.clusters
    consumer2_clusters["secondary"] = consumer2_clusters["primary"]
    consumer2_clusters.pop("primary")

    node2_clusters = stage1.nodes["node2"].clusters
    node2_clusters["secondary"] = node2_clusters["primary"]
    node2_clusters.pop("primary")

    table_details = DescriptionDetails(description)
    stage1_all = table_details.get_prepared_stage_descriptions(
        "stage1", {StageEntityType.TABLE: {"all"}, StageEntityType.CONSUMER: {"all"}, StageEntityType.NODE: {"all"}}
    )
    assert len(stage1_all) == 2

    assert len(stage1_all[0].tables) == 1
    assert len(stage1_all[0].consumers) == 1
    assert len(stage1_all[0].nodes) == 1
    assert stage1_all[0].locks == (custom_locks if custom_locks else ["//tmp/db_folder"])

    assert len(stage1_all[1].tables) == 1
    assert len(stage1_all[1].consumers) == 1
    assert len(stage1_all[1].nodes) == 1
    assert stage1_all[1].locks == (custom_locks if custom_locks else ["//tmp/db_folder"])
