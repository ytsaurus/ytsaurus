EXACTLY_ONCE_SINK = "NYT::NFlow::TClickHouseBatchingSink"
SHARDED_EXACTLY_ONCE_SINK = "NYT::NFlow::TShardedClickHouseBatchingSink"
AT_LEAST_ONCE_SINK = "NYT::NFlow::TAtLeastOnceClickHouseSink"
AT_MOST_ONCE_SINK = "NYT::NFlow::TAtMostOnceClickHouseSink"


def make_clickhouse_sink(
    host=None,
    table=None,
    *,
    sink_class_name=EXACTLY_ONCE_SINK,
    input_stream_ids,
    hosts=None,
    shard_hosts=None,
    sharding_key_columns=None,
    host_selection_policy=None,
    port=9000,
    user="default",
    password_env_var="",
    database="default",
):
    host_forms = {
        "host": host,
        "hosts": hosts,
        "shard_hosts": shard_hosts,
    }
    if sum(value is not None for value in host_forms.values()) != 1:
        raise ValueError("exactly one of host, hosts, and shard_hosts must be set")
    if host == "":
        raise ValueError('"host" must not be empty')
    if hosts is not None:
        if any(replica_host == "" for replica_host in hosts):
            raise ValueError('"hosts" contains an empty host')
        if len(hosts) == 1:
            raise ValueError('"hosts" with a single entry is just "host"; use "host" instead')
    if shard_hosts is not None:
        for shard_name, shard_replicas in shard_hosts.items():
            if any(replica_host == "" for replica_host in shard_replicas):
                raise ValueError(f"shard {shard_name!r} has an empty host")
    if table is None:
        raise ValueError("table must be set")
    if sink_class_name == SHARDED_EXACTLY_ONCE_SINK and shard_hosts is None:
        raise ValueError("The sharded exactly-once sink requires shard_hosts")
    if sink_class_name == EXACTLY_ONCE_SINK and shard_hosts is not None:
        raise ValueError("The unsharded exactly-once sink does not accept shard_hosts")
    if sharding_key_columns is not None and shard_hosts is None:
        raise ValueError("sharding_key_columns requires shard_hosts")

    parameters = {
        "port": port,
        "user": user,
        "database": database,
        "table": table,
    }
    parameters.update((name, value) for name, value in host_forms.items() if value is not None)
    if sharding_key_columns is not None:
        parameters["sharding_key_columns"] = list(sharding_key_columns)
    if host_selection_policy is not None:
        parameters["host_selection_policy"] = host_selection_policy
    if password_env_var:
        parameters["password_env_var"] = password_env_var

    return {
        "sink_class_name": sink_class_name,
        "input_stream_ids": list(input_stream_ids),
        "parameters": parameters,
    }
