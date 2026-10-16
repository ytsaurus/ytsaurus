from library.python import resource
import yt.yson as yson

from yt.yt.flow.library.python.pipeline_tables import PIPELINE_FILES
from yt.yt.flow.library.python.pipeline_tables import PIPELINE_QUEUES
from yt.yt.flow.library.python.pipeline_tables import PIPELINE_TABLES
from yt.yt.flow.library.python.pipeline_tables import definitions as definitions_module
from yt.yt.flow.library.python.pipeline_tables.definitions import _DEFINITIONS
from yt.yt.flow.library.python.pipeline_tables.definitions import _get_pipeline_table_definitions
from yt.yt.flow.library.python.pipeline_tables.definitions import _RESOURCE_KEY


def test_source_tree_fallback_matches_resource(monkeypatch, tmp_path):
    module_path = tmp_path / "library/python/pipeline_tables/definitions.py"
    source_path = tmp_path / "library/pipeline_tables/definitions.yson"
    source_path.parent.mkdir(parents=True)
    source_path.write_bytes(resource.find(_RESOURCE_KEY))

    monkeypatch.setattr(definitions_module, "_resource", None)
    monkeypatch.setattr(definitions_module, "__file__", module_path)

    assert definitions_module._load_definitions() == _DEFINITIONS


def test_resource_builds_independent_full_and_legacy_views():
    definitions = yson.loads(resource.find(_RESOURCE_KEY))
    table_definitions, queue_definitions = _get_pipeline_table_definitions()

    assert PIPELINE_FILES == []
    for section_name, exported, full_definitions in (
        ("tables", PIPELINE_TABLES, table_definitions),
        ("queues", PIPELINE_QUEUES, queue_definitions),
    ):
        section = definitions[section_name]
        assert set(exported) == set(section)
        for name, descriptor in section.items():
            schema = exported[str(name)]["schema"]
            assert set(exported[str(name)]) == {"schema"}
            assert schema == descriptor["schema"]
            assert schema.attributes == descriptor["schema"].attributes
            assert full_definitions[str(name)] == descriptor
            assert full_definitions[str(name)]["schema"] is not _DEFINITIONS[section_name][name]["schema"]
            assert full_definitions[str(name)]["attributes"] is not _DEFINITIONS[section_name][name]["attributes"]
            assert schema is not full_definitions[str(name)]["schema"]

    raw_column = _DEFINITIONS["tables"]["input_messages"]["schema"][0]
    full_column = table_definitions["input_messages"]["schema"][0]
    legacy_column = PIPELINE_TABLES["input_messages"]["schema"][0]
    assert raw_column is not full_column
    assert raw_column is not legacy_column
    assert full_column is not legacy_column

    original_group = raw_column["group"]
    full_column["group"] = "changed"
    assert raw_column["group"] == original_group
    assert legacy_column["group"] == original_group
    full_column["group"] = original_group

    legacy_column["group"] = "changed"
    assert raw_column["group"] == original_group
    assert full_column["group"] == original_group
    legacy_column["group"] = original_group

    raw_mount_config = _DEFINITIONS["tables"]["input_messages"]["attributes"]["mount_config"]
    full_mount_config = table_definitions["input_messages"]["attributes"]["mount_config"]
    assert raw_mount_config is not full_mount_config

    original_row_merger = raw_mount_config["row_merger_type"]
    full_mount_config["row_merger_type"] = "changed"
    assert raw_mount_config["row_merger_type"] == original_row_merger
    full_mount_config["row_merger_type"] = original_row_merger
