import uuid

import pytest


@pytest.fixture
def table_path() -> str:
    return "//tmp/db_folder/table_%s" % uuid.uuid4()


@pytest.fixture
def folder_path() -> str:
    return "//tmp/db_folder/folder_%s" % uuid.uuid4()


@pytest.fixture
def default_schema() -> list[dict[str, str]]:
    return [
        {"expression": "farm_hash(Key) % 10", "name": "Hash", "sort_order": "ascending", "type": "uint64"},
        {"name": "Key", "sort_order": "ascending", "type": "uint64"},
        {"name": "Value", "type": "uint64", "lock": "api"},
    ]


@pytest.fixture
def default_schema_broad_key() -> list[dict[str, str]]:
    return [
        {"expression": "farm_hash(Key) % 10", "name": "Hash", "sort_order": "ascending", "type": "uint64"},
        {"name": "Key", "sort_order": "ascending", "type": "uint64"},
        {"name": "SubKey", "sort_order": "ascending", "type": "string"},
        {"name": "Value", "type": "uint64", "lock": "api"},
    ]


@pytest.fixture
def ordered_schema() -> list[dict[str, str]]:
    return [
        {"name": "$timestamp", "type": "uint64"},
        {"name": "Key", "type": "uint64"},
        {"name": "Value", "type": "uint64"},
    ]
