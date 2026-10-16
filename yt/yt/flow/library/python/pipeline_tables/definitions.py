from copy import deepcopy
from pathlib import Path

import yt.yson as yson

try:
    from library.python import resource as _resource
except ModuleNotFoundError:
    _resource = None

_RESOURCE_KEY = "resfs/file/yt/yt/flow/library/pipeline_tables/definitions.yson"


def _load_section(definitions, section):
    return {
        str(name): {
            "schema": deepcopy(descriptor["schema"]),
            "attributes": deepcopy(descriptor["attributes"]),
        }
        for name, descriptor in definitions[section].items()
    }


def _schemas_only(definitions):
    return {name: {"schema": deepcopy(descriptor["schema"])} for name, descriptor in definitions.items()}


def _load_definitions():
    if _resource is not None:
        return yson.loads(_resource.find(_RESOURCE_KEY))

    try:
        data = Path(__file__).with_name("definitions.yson").read_bytes()
    except FileNotFoundError:
        source_path = Path(__file__).resolve().parents[2] / "pipeline_tables" / "definitions.yson"
        data = source_path.read_bytes()
    return yson.loads(data)


def _get_pipeline_table_definitions():
    return (
        _load_section(_DEFINITIONS, "tables"),
        _load_section(_DEFINITIONS, "queues"),
    )


_DEFINITIONS = _load_definitions()

PIPELINE_FILES = []
PIPELINE_TABLES = _schemas_only(_DEFINITIONS["tables"])
PIPELINE_QUEUES = _schemas_only(_DEFINITIONS["queues"])
