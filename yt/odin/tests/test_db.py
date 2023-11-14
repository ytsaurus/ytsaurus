from yt_odin.storage.db import create_yt_table_client_for_cluster, create_yt_table_client, init_yt_table
from yt_odin.test_helpers import yt_env  # noqa


def _check_add_get_records(client):
    first_record = dict(
        cluster="hahahn",
        service="foo",
        timestamp=3,
        state=1.0,
        messages="haha",
    )
    second_record = dict(
        cluster="markoff",
        service="bar",
        timestamp=4,
        state=1.0,
        duration=450,
    )
    client.add_record(check_id=1, **first_record)
    client.add_record(check_id=2, **second_record)
    reply = client.get_records(clusters=first_record["cluster"], services=first_record["service"],
                               start_timestamp=1, stop_timestamp=5)
    assert len(reply) == 1
    _check_record(reply[0], dict(duration=None, **first_record))
    reply = client.get_records(
        clusters=[first_record["cluster"], second_record["cluster"]],
        services="service", start_timestamp=1, stop_timestamp=5)
    assert not reply
    reply = client.get_records(
        clusters=[first_record["cluster"], second_record["cluster"]],
        services=[first_record["service"], second_record["service"]],
        start_timestamp=1, stop_timestamp=5)
    assert len(reply) == 2
    reply.sort(key=lambda x: x[2])  # sort by timestamp
    _check_record(reply[0], dict(duration=None, **first_record))
    _check_record(reply[1], dict(messages=None, **second_record))


def _check_add_get_records_for_cluster(client, cluster):
    record = dict(
        service="service",
        timestamp=2,
        state=-1.0,
        duration=790,
        messages="looong string\n" * 10000,
    )
    client.add_record(check_id=1, **record)
    reply = client.get_records(record["service"], start_timestamp=1, stop_timestamp=3)
    assert len(reply) == 1
    _check_record(reply[0], dict(cluster=cluster, **record))
    reply = client.get_records(services="grep", start_timestamp=1, stop_timestamp=3)
    assert not reply
    reply = client.get_records(record["service"], start_timestamp=2, stop_timestamp=2)
    assert len(reply) == 1
    _check_record(reply[0], dict(cluster=cluster, **record))


def _check_record(record, reference):
    EPS = 0.001
    for key, reference_value in reference.items():
        value = getattr(record, key)
        if isinstance(reference_value, float):
            assert abs(reference_value - value) < EPS
        else:
            assert reference_value == value


def test_yt_db(yt_env):  # noqa
    client = yt_env.yt_client
    TABLE = "//tmp/test_table_one"
    init_yt_table(TABLE, client, bundle="default")
    proxy = client.config["proxy"]["url"]
    token = client.config["token"]
    db_table_client = create_yt_table_client(table=TABLE, proxy=proxy, token=token)
    _check_add_get_records(db_table_client)


def test_yt_db_for_cluster(yt_env):  # noqa
    client = yt_env.yt_client
    TABLE = "//tmp/test_table_two"
    CLUSTER = "hahahn"
    init_yt_table(TABLE, client, bundle="default")
    proxy = client.config["proxy"]["url"]
    token = client.config["token"]
    db_table_client_for_cluster = create_yt_table_client_for_cluster(table=TABLE, cluster=CLUSTER,
                                                                     proxy=proxy, token=token)
    _check_add_get_records_for_cluster(db_table_client_for_cluster, CLUSTER)
