import pytest

from yt.yt.flow.extensions.clickhouse.python import (
    SHARDED_EXACTLY_ONCE_SINK,
    make_clickhouse_sink,
)


def test_make_sharded_clickhouse_sink():
    sink = make_clickhouse_sink(
        table="events",
        sink_class_name=SHARDED_EXACTLY_ONCE_SINK,
        input_stream_ids=["input"],
        shard_hosts={"east": ["east-1", "east-2"], "west": ["west-1"]},
        sharding_key_columns=["user_id"],
        host_selection_policy="random_start",
    )

    assert sink == {
        "sink_class_name": SHARDED_EXACTLY_ONCE_SINK,
        "input_stream_ids": ["input"],
        "parameters": {
            "shard_hosts": {"east": ["east-1", "east-2"], "west": ["west-1"]},
            "sharding_key_columns": ["user_id"],
            "host_selection_policy": "random_start",
            "port": 9000,
            "user": "default",
            "database": "default",
            "table": "events",
        },
    }


def test_make_clickhouse_sink_rejects_multiple_host_forms():
    with pytest.raises(ValueError, match="exactly one"):
        make_clickhouse_sink(
            "localhost",
            "events",
            input_stream_ids=["input"],
            hosts=["replica-1", "replica-2"],
        )


@pytest.mark.parametrize(
    ("host_parameters", "error"),
    [
        ({"host": ""}, '"host" must not be empty'),
        ({"hosts": ["only-host"]}, 'use "host" instead'),
        ({"hosts": ["replica-1", ""]}, '"hosts" contains an empty host'),
        (
            {
                "sink_class_name": SHARDED_EXACTLY_ONCE_SINK,
                "shard_hosts": {"east": ["east-1"], "west": [""]},
            },
            "has an empty host",
        ),
    ],
    ids=["empty_host", "single_host_list", "empty_host_list_entry", "empty_shard_host_entry"],
)
def test_make_clickhouse_sink_rejects_invalid_host_values(host_parameters, error):
    with pytest.raises(ValueError, match=error):
        make_clickhouse_sink(
            table="events",
            input_stream_ids=["input"],
            **host_parameters,
        )
