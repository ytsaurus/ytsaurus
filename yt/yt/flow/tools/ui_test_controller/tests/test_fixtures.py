import hashlib
import pathlib

import yatest.common as ycommon

from yt.wrapper import yson

EXPECTED_STATES = {
    "stopped_clean": "stopped",
    "working_healthy": "working",
    "paused_with_backlog": "paused",
    "draining_with_backlog": "draining",
    "draining_drained": "draining",
    "stopped_after_drain": "stopped",
    "completed": "completed",
}

CAPTURE_SOURCE_FILES = (
    "test_capture.py",
    "lib/computation.cpp",
    "lib/computation.h",
    "pipeline/pipeline.yson",
)


def _load(path):
    with path.open("rb") as stream:
        return yson.load(stream)


def _capture_source_sha256():
    digest = hashlib.sha256()
    root = "yt/yt/flow/tools/ui_test_controller/fixture_pipeline"
    for relative_path in CAPTURE_SOURCE_FILES:
        digest.update(relative_path.encode())
        digest.update(b"\0")
        digest.update(pathlib.Path(ycommon.source_path(f"{root}/{relative_path}")).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _streams(view):
    return [
        stream
        for computation in view["state"]["traverse_data"]["computations"].values()
        for stream in computation["streams"].values()
    ]


def _has_runtime_errors(view):
    feedback = view["feedback"]
    for status in feedback["partition_job_statuses"].values():
        job_status = status.get("current_job_status")
        if not job_status:
            continue
        if int(job_status.get("error", {}).get("code", 0)) != 0 or job_status.get("retryable_errors"):
            return True
    for status in feedback["worker_statuses"].values():
        if status.get("errors") or int(status.get("previous_crash_error", {}).get("code", 0)) != 0:
            return True
    return False


def test_live_fixture_resource():
    extracted = pathlib.Path(ycommon.output_path("live_fixtures"))
    extracted.mkdir()
    ycommon.execute(
        [
            "tar",
            "--zstd",
            "-xf",
            ycommon.work_path("flow-ui-live-fixtures.tar.zst"),
            "-C",
            str(extracted),
        ]
    )
    assert not any(path.is_symlink() for path in extracted.rglob("*"))

    metadata = _load(extracted / "capture_metadata.yson")
    assert metadata["source"] == "live-flow-integration-test"
    assert metadata["pipeline_target"] == "yt/yt/flow/tools/ui_test_controller/fixture_pipeline"
    assert metadata["source_sha256"] == _capture_source_sha256()
    assert int(metadata["format_version"]) == 2
    assert metadata["runtime_identity"] == "sanitized"
    assert metadata["safety_check"] == "environment-and-structure-v1"

    scenarios = (extracted / "scenarios.txt").read_text().splitlines()
    assert scenarios == list(EXPECTED_STATES)
    assert int((extracted / "now_seconds.txt").read_text()) > 0

    captured = {}
    for scenario, expected_state in EXPECTED_STATES.items():
        view = _load(extracted / "fixtures" / f"{scenario}.yson")
        orchids = _load(extracted / "job_orchids" / f"{scenario}.yson")
        state = str(view["state"]["execution_spec"]["pipeline_state"]["value"])
        assert state == expected_state
        assert view["current_spec"]["value"]["computations"]
        captured[scenario] = (view, orchids)

    working, orchids = captured["working_healthy"]
    layout = working["state"]["execution_spec"]["layout"]
    workers = working["state"]["workers"]
    assert set(workers) == {"localhost:19021", "localhost:19022"}
    assert all(str(worker["address"]) == str(address) for address, worker in workers.items())
    assert all(str(worker["rpc_address"]) == str(address) for address, worker in workers.items())
    assert {str(worker["name"]) for worker in workers.values()} == {"localhost"}
    assert {str(worker["monitoring_address"]) for worker in workers.values()} == {
        "localhost:19121",
        "localhost:19122",
    }
    assert {str(worker["build_version"]) for worker in workers.values()} == {"ui-test-build"}
    assert {str(worker["remote_shell_command"]) for worker in workers.values()} == {"ssh localhost"}
    assert {str(job["worker_address"]) for job in layout["jobs"].values()} <= set(workers)
    assert layout["partitions"]
    assert layout["jobs"]
    expected_orchids = {
        str(partition_id) for partition_id, partition in layout["partitions"].items() if partition.get("current_job_id")
    }
    assert set(orchids) <= expected_orchids
    assert not _has_runtime_errors(working)

    stopped_clean, _ = captured["stopped_clean"]
    assert not stopped_clean["state"]["execution_spec"]["layout"]["jobs"]

    draining, _ = captured["draining_drained"]
    assert any(str(stream["state"]) == "drained" for stream in _streams(draining))

    stopped_after_drain, _ = captured["stopped_after_drain"]
    stopped_layout = stopped_after_drain["state"]["execution_spec"]["layout"]
    assert stopped_layout["partitions"]
    assert not stopped_layout["jobs"]
    assert all(str(stream["state"]) in ("drained", "completed") for stream in _streams(stopped_after_drain))
