import pytest


def test_read_target_attribute(example_env):
    publisher = example_env.client.create_object(
        "publisher",
        {"spec": {"address": "Yekaterinburg", "number_of_awards": "33"}},
        enable_structured_response=True,
        request_meta_response=True,
    )["meta"]["id"]

    example_env.client.update_object(
        "publisher",
        str(publisher),
        set_updates=[
            {"path": "/spec/address", "value": "Moscow"},
            {"path": "/spec/number_of_awards", "value": "3"},
        ],
    )

    result = example_env.client.get_object(
        "publisher",
        str(publisher),
        selectors=["/meta", "/status/address", "/status/number_of_awards"],
    )
    assert result[1] == "Moscow"
    assert result[2] == 3


def test_update_target_no_overwrite(example_env):
    publisher = example_env.client.create_object(
        "publisher",
        {"spec": {"address": "Yekaterinburg"}},
        enable_structured_response=True,
        request_meta_response=True,
    )["meta"]["id"]
    example_env.client.update_object(
        "publisher",
        str(publisher),
        set_updates=[
            {"path": "/status/address", "value": "Moscow"},
        ],
    )

    result = example_env.client.get_object(
        "publisher",
        str(publisher),
        selectors=["/meta", "/spec/address", "/status/address"],
    )
    assert result[1] == "Yekaterinburg"
    assert result[2] == "Moscow"


def test_control_migrate_attributes(example_env):
    publisher = example_env.client.create_object(
        "publisher",
        {"spec": {"address": "Yekaterinburg"}, "status": {"address": "Moscow"}},
        enable_structured_response=True,
        request_meta_response=True,
    )["meta"]["id"]

    result = example_env.client.get_object(
        "publisher",
        str(publisher),
        selectors=["/meta", "/spec/address", "/status/address"],
    )
    assert result[1] == "Yekaterinburg"
    assert result[2] == "Moscow"

    example_env.client.update_object(
        "publisher",
        str(publisher),
        set_updates=[
            {"path": "/control/migrate_attributes", "value": {"target_paths": ["/status/address"]}},
        ],
    )

    result = example_env.client.get_object(
        "publisher",
        str(publisher),
        selectors=["/meta", "/spec/address", "/status/address"],
    )
    assert result[1] == "Yekaterinburg"
    assert result[2] == "Yekaterinburg"


def test_custom_migrator(example_env):
    publisher = example_env.client.create_object(
        "publisher",
        {},
        enable_structured_response=True,
        request_meta_response=True,
    )["meta"]["id"]

    example_env.client.update_object(
        "publisher",
        str(publisher),
        set_updates=[
            {"path": "/spec/number_of_awards", "value": "hello world!"},
        ],
    )

    result = example_env.client.get_object(
        "publisher",
        str(publisher),
        selectors=["/meta", "/spec/number_of_awards", "/status/number_of_awards"],
    )
    assert result[1] == "hello world!"
    assert result[2] == -1


def test_on_conflict_policy_use_target(example_env):
    publisher = example_env.client.create_object(
        "publisher",
        {},
        enable_structured_response=True,
        request_meta_response=True,
    )["meta"]["id"]

    example_env.client.update_object(
        "publisher",
        str(publisher),
        set_updates=[
            {"path": "/spec/number_of_awards", "value": "hello world!"},
            {"path": "/status/number_of_awards", "value": 8},
        ],
    )

    result = example_env.client.get_object(
        "publisher",
        str(publisher),
        selectors=["/meta", "/spec/number_of_awards", "/status/number_of_awards"],
    )
    assert result[1] == "8"
    assert result[2] == 8


def test_migrate_on_create(example_env):
    publisher = example_env.client.create_object(
        "publisher",
        {"spec": {"address": "Yekaterinburg", "number_of_awards": "10"}},
        enable_structured_response=True,
        request_meta_response=True,
    )["meta"]["id"]

    result = example_env.client.get_object(
        "publisher",
        str(publisher),
        selectors=["/meta", "/spec/address", "/status/address", "/spec/number_of_awards", "/status/number_of_awards"],
    )
    assert result[1] == "Yekaterinburg"
    assert result[2] == "Yekaterinburg"
    assert result[3] == "10"
    assert result[4] == 10


def test_no_conflict_on_create(example_env):
    publisher = example_env.client.create_object(
        "publisher",
        {
            "spec": {"address": "Yekaterinburg", "number_of_awards": "10"},
            "status": {"address": "Moscow", "number_of_awards": 15},
        },
        enable_structured_response=True,
        request_meta_response=True,
    )["meta"]["id"]

    result = example_env.client.get_object(
        "publisher",
        str(publisher),
        selectors=["/meta", "/spec/address", "/status/address", "/spec/number_of_awards", "/status/number_of_awards"],
    )
    assert result[1] == "Yekaterinburg"
    assert result[2] == "Moscow"
    assert result[3] == "15"
    assert result[4] == 15


@pytest.mark.parametrize(
    "status_etc",
    [
        {},
        {"address": "Moscow"},
    ],
)
def test_get_old_reads_old_target(example_env, status_etc):
    PUBLISHERS_TABLE_PATH = "//home/example/db/publishers"
    yt_client = example_env.yt_client
    publisher = example_env.client.create_object(
        "publisher",
        {
            "spec": {"address": "Yekaterinburg"},
        },
        enable_structured_response=True,
        request_meta_response=True,
    )["meta"]["id"]

    rows = list(yt_client.select_rows(f"* from [{PUBLISHERS_TABLE_PATH}] WHERE [meta.id] = {publisher} LIMIT 1"))
    assert len(rows) == 1, "Object row not found"
    del rows[0]["hash"]
    rows[0]["status.etc"] = status_etc
    # Update row manually to simulate state before adding a migration.
    yt_client.insert_rows(PUBLISHERS_TABLE_PATH, rows)
    result = example_env.client.get_object(
        "publisher",
        str(publisher),
        selectors=["/meta", "/spec/address", "/status/address"],
    )
    assert result[1] == "Yekaterinburg"
    assert result[2] == status_etc.get("address")

    example_env.client.update_object(
        "publisher",
        str(publisher),
        set_updates=[
            {"path": "/spec/address", "value": "Saint Petersburg"},
        ],
    )
    result = example_env.client.get_object(
        "publisher",
        str(publisher),
        selectors=["/meta", "/spec/address", "/status/address"],
    )
    assert result[1] == "Saint Petersburg"
    assert result[2] == "Saint Petersburg"


def test_select_by_target_non_column_works_after_migration(example_env):
    publisher = example_env.client.create_object(
        "publisher",
        {
            "spec": {"number_of_awards": "1000"},
        },
        enable_structured_response=True,
        request_meta_response=True,
    )["meta"]["id"]

    result = example_env.client.select_objects(
        "publisher",
        filter="[/status/number_of_awards]=1000",
        index="publishers_by_number_of_awards",
        selectors=["/meta/id"],
    )
    assert len(result) == 1
    assert result[0][0] == publisher


def test_select_by_target_column_works_after_migration(example_env):
    publisher = example_env.client.create_object(
        "publisher",
        {
            "spec": {"non_column_field": 111},
        },
        enable_structured_response=True,
        request_meta_response=True,
    )["meta"]["id"]
    result = example_env.client.select_objects(
        "publisher", filter="[/spec/column_field]=111", index="publishers_by_column_field", selectors=["/meta/id"]
    )
    assert len(result) == 1
    assert result[0][0] == publisher


def test_reverse_write(example_env):
    publisher = example_env.client.create_object(
        "publisher",
        {
            "spec": {"number_of_awards": "4"},
        },
        enable_structured_response=True,
        request_meta_response=True,
    )["meta"]["id"]

    example_env.client.update_object(
        "publisher",
        str(publisher),
        set_updates=[
            {"path": "/status/number_of_awards", "value": 8},
        ],
    )

    result = example_env.client.get_object(
        "publisher",
        str(publisher),
        selectors=["/meta", "/spec/number_of_awards", "/status/number_of_awards"],
    )
    assert result[1] == "8"
    assert result[2] == 8


def test_unlink_then_remove_foreign_in_transaction(example_env):
    illustrator = example_env.create_illustrator()
    publisher = example_env.client.create_object(
        "publisher",
        {
            "spec": {"featured_illustrators": [illustrator]}
        }
    )
    tx = example_env.client.start_transaction()
    example_env.client.update_object(
        "publisher",
        str(publisher),
        remove_updates=[
            {"path": "/spec/featured_illustrators"},
        ],
        transaction_id=tx,
    )
    example_env.client.remove_object(
        "illustrator",
        str(illustrator),
        transaction_id=tx,
    )
    example_env.client.commit_transaction(tx)
