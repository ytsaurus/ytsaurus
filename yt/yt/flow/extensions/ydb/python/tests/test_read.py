from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from yt.yt.flow.extensions.ydb.python import read_all_from_topic
from yt.yt.flow.extensions.ydb.python.codecs import DECODERS


def make_partition(partition_id, start, end):
    return SimpleNamespace(
        partition_id=partition_id,
        partition_stats=SimpleNamespace(partition_start=start, partition_end=end),
    )


def make_message(partition_id, offset, data):
    return SimpleNamespace(
        partition_id=partition_id,
        _partition_session=SimpleNamespace(partition_id=partition_id),
        offset=offset,
        data=data,
    )


def make_driver(partitions, messages):
    driver = MagicMock()
    driver.topic_client.describe_topic.return_value = SimpleNamespace(partitions=partitions)
    reader = driver.topic_client.reader.return_value.__enter__.return_value
    reader.receive_message.side_effect = messages
    return driver, reader


@pytest.mark.authors(["pechatnov"])
@pytest.mark.parametrize("detailed", [False, True])
@pytest.mark.parametrize("commit", [False, True])
def test_read_snapshot_ignores_new_messages_in_other_partitions(detailed, commit):
    first = make_message(1, 3, b"first")
    last = make_message(1, 4, b"last")
    heartbeat = make_message(7, 0, b"")
    new_heartbeat = make_message(7, 1, b"")
    empty_partition_heartbeat = make_message(11, 9, b"")
    messages = [first, heartbeat, new_heartbeat, empty_partition_heartbeat, last]
    driver, reader = make_driver(
        [make_partition(1, 3, 5), make_partition(7, 0, 1), make_partition(11, 9, 9)],
        messages,
    )

    result = read_all_from_topic(driver, "topic", "consumer", commit=commit, detailed=detailed)

    expected = {1: [first, last], 7: [heartbeat]} if detailed else [b"first", b"last", b""]
    assert result == expected
    assert reader.receive_message.call_count == len(messages)
    assert reader.commit_with_ack.call_args_list == ([call(first), call(last), call(heartbeat)] if commit else [])


@pytest.mark.authors(["pechatnov"])
@pytest.mark.parametrize("detailed", [False, True])
def test_read_empty_snapshot(detailed):
    driver, reader = make_driver([make_partition(7, 9, 9)], [])

    assert read_all_from_topic(driver, "topic", "consumer", commit=True, detailed=detailed) == ({} if detailed else [])
    reader.receive_message.assert_not_called()
    reader.commit_with_ack.assert_not_called()


def test_reader_uses_public_zstd_decoders():
    driver, _ = make_driver([make_partition(7, 9, 9)], [])

    read_all_from_topic(driver, "topic", "consumer")

    driver.topic_client.reader.assert_called_once_with("topic", "consumer", decoders=DECODERS)
