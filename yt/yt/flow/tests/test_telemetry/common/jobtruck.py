from yt.common import wait

JOBTRUCK_TABLE_NAMES = ("logs/sources_meta", "logs/logs_meta", "logs/logs")
_SOURCE_ID = "itflow_v0:controller_leader"
_DESCRIPTOR_KEYS = ("source_id", "instance_id", "log_name", "file_path")


def read_jobtruck_logs(client, pipeline_path, log_limit=None):
    rows = {}
    for path in JOBTRUCK_TABLE_NAMES:
        name = path.rsplit("/", 1)[1]
        limit = f" limit {log_limit}" if name == "logs" and log_limit is not None else ""
        rows[name] = list(client.select_rows(f"* from [{pipeline_path}/{path}]{limit}"))
    return rows


def assert_jobtruck_tables(client, pipeline_path, table_type, *, replica_cluster=None, replica_client=None):
    assert (replica_cluster is None) == (replica_client is None)
    assert client.get(f"{pipeline_path}/logs/@yt_logs_format_version") == 1
    for name in JOBTRUCK_TABLE_NAMES:
        path = f"{pipeline_path}/{name}"
        assert client.get(f"{path}/@type") == table_type
        if table_type != "chaos_replicated_table":
            assert client.get(f"{path}/@dynamic")
        if replica_client is None:
            continue
        replicas = client.get(f"{path}/@replicas")
        if table_type == "chaos_replicated_table":
            replicas = {
                replica_id: replica for replica_id, replica in replicas.items() if replica["content_type"] == "data"
            }
        assert len(replicas) == 1, replicas
        replica_id, replica = next(iter(replicas.items()))
        assert replica["cluster_name"] == replica_cluster
        assert replica["replica_path"] == path
        assert replica["mode"] == "sync"
        assert replica["state"] == "enabled"
        assert replica_client.get(f"{path}/@type") == "table"
        assert replica_client.get(f"{path}/@dynamic")
        assert replica_client.get(f"{path}/@upstream_replica_id") == replica_id


def check_jobtruck_logs(client, pipeline_path, node_infos=None, *, replica_client=None, check_progress=True):
    if node_infos is None:
        leader_info = {}

        def capture_leader():
            rows = list(client.select_rows(f'* from [{pipeline_path}/flow_control] where key = "leader_controller"'))
            if len(rows) != 1:
                return False
            leader_info.update(rows[0]["value"])
            return True

        wait(capture_leader, timeout=180, ignore_exceptions=True)
        node_infos = [leader_info]
    expected_sources = {
        (_SOURCE_ID, str(info["incarnation_id"])): {
            "kind": "yt_flow_controller",
            "display_name": "Flow controller leader",
            "node_info": info,
        }
        for info in node_infos
    }
    assert expected_sources and len(expected_sources) == len(node_infos)
    expected_descriptors = {
        (source_id, instance_id, "controller", "/logs/controller.log") for source_id, instance_id in expected_sources
    }
    captured = {}
    latest_by_instance = {}

    def capture_logs():
        rows = read_jobtruck_logs(client, pipeline_path)
        sources = {(row["source_id"], row["instance_id"]): row["meta"] for row in rows["sources_meta"]}
        meta_descriptors = {tuple(row[key] for key in _DESCRIPTOR_KEYS) for row in rows["logs_meta"]}
        nonempty_logs = [row for row in rows["logs"] if row["line"]]
        log_descriptors = {tuple(row[key] for key in _DESCRIPTOR_KEYS) for row in rows["logs"]}
        nonempty_descriptors = {tuple(row[key] for key in _DESCRIPTOR_KEYS) for row in nonempty_logs}
        if (
            sources != expected_sources
            or meta_descriptors != expected_descriptors
            or log_descriptors != expected_descriptors
            or nonempty_descriptors != expected_descriptors
            or any(row["log_config"] != {"log_level": "INFO"} or row["creation_time"] <= 0 for row in rows["logs_meta"])
            or any(row["ts"] <= 0 for row in nonempty_logs)
        ):
            return False
        latest_by_instance.clear()
        for row in nonempty_logs:
            instance = (row["source_id"], row["instance_id"])
            if instance not in latest_by_instance or row["ts"] > latest_by_instance[instance]["ts"]:
                latest_by_instance[instance] = row
        captured.update(
            sources_meta=rows["sources_meta"], logs_meta=rows["logs_meta"], logs=list(latest_by_instance.values())
        )
        return True

    wait(capture_logs, timeout=180, ignore_exceptions=True)
    if check_progress:

        def metadata_timestamps():
            timestamps = {}
            metadata_client = replica_client if replica_client is not None else client
            for name, keys in (
                ("sources_meta", ("source_id", "instance_id")),
                ("logs_meta", _DESCRIPTOR_KEYS),
            ):
                rows = metadata_client.lookup_rows(
                    f"{pipeline_path}/logs/{name}",
                    [{key: row[key] for key in keys} for row in captured[name]],
                    versioned=True,
                )
                timestamps[name] = [row.attributes["write_timestamps"][0] for row in rows]
            return timestamps

        first_metadata_timestamps = metadata_timestamps()
        instance = (_SOURCE_ID, str(node_infos[-1]["incarnation_id"]))
        first_timestamp = latest_by_instance[instance]["ts"]
        wait(
            lambda: capture_logs() and latest_by_instance[instance]["ts"] > first_timestamp,
            timeout=180,
            ignore_exceptions=True,
        )
        assert metadata_timestamps() == first_metadata_timestamps
    if replica_client is not None:

        def captured_rows_visible():
            replica_rows = read_jobtruck_logs(replica_client, pipeline_path)
            return all(row in replica_rows[name] for name, rows in captured.items() for row in rows)

        wait(captured_rows_visible, timeout=180, ignore_exceptions=True)
