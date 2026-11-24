import hashlib
import json
import os
import signal
import socket
import stat
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path

import pytest
import yatest.common

from library.python.port_manager import PortManager

from . import public_ydb
from .public_ydb import (
    OwnedProcessGroup,
    PublicYdbInstance,
    file_sha256,
    fixture_source_path,
    load_lock,
    locate_fixture_binaries,
    render_config,
    resolve_binary,
    resolve_cli,
)

PROBE_PATH = "yt/yt/flow/extensions/ydb/test/probe/ydb_fixture_probe"


def process_exists(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return process_state(pid) != "Z"


def process_state(pid):
    return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]


def wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.05)


def open_file_paths():
    paths = set()
    for descriptor in os.listdir("/proc/self/fd"):
        try:
            paths.add(os.readlink(f"/proc/self/fd/{descriptor}"))
        except FileNotFoundError:
            pass
    return paths


def assert_port_free(port):
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("::", port))


def write_executable(path, body):
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(0o700)
    return path


@pytest.fixture
def work_directory(request):
    directory = Path(yatest.common.output_path(request.node.name))
    directory.mkdir(parents=True, exist_ok=True)
    return directory


@pytest.fixture
def fast_drain(monkeypatch):
    monkeypatch.setattr(public_ydb, "GROUP_DRAIN_SECONDS", 1)


def start_group(directory, script):
    group = OwnedProcessGroup(["/bin/sh", "-c", script], directory, directory / "group.log")
    group.start()
    return group


def read_pid(path):
    wait_until(lambda: path.exists() and path.read_text().strip())
    return int(path.read_text())


def assert_group_stopped(group, pids, log_path):
    process_group = group.process_group
    group.stop()
    assert group.process_group is None
    with pytest.raises(ProcessLookupError):
        os.killpg(process_group, 0)
    assert not any(process_exists(pid) for pid in pids)
    assert str(log_path) not in open_file_paths()


def test_group_stop_after_normal_exit(work_directory):
    group = start_group(work_directory, "exit 0")
    leader = group.process_group
    wait_until(lambda: not group.leader_alive())
    assert_group_stopped(group, [leader], work_directory / "group.log")


def test_group_stop_running_leader(work_directory):
    group = start_group(work_directory, "exec sleep 1000")
    assert_group_stopped(group, [group.process_group], work_directory / "group.log")


def test_group_stop_after_sigstop(work_directory):
    group = start_group(work_directory, f"sleep 1000 & echo $! > {work_directory}/child; wait")
    child = read_pid(work_directory / "child")
    leader = group.process_group
    group.signal(signal.SIGSTOP)
    wait_until(lambda: process_state(child) == "T")
    assert_group_stopped(group, [leader, child], work_directory / "group.log")


def test_group_stop_kills_term_resistant_descendant(work_directory, fast_drain):
    group = start_group(work_directory, f"trap '' TERM; sleep 1000 & echo $! > {work_directory}/child; wait")
    child = read_pid(work_directory / "child")
    assert_group_stopped(group, [group.process_group, child], work_directory / "group.log")


def test_group_stop_after_leader_exits_with_live_descendant(work_directory):
    group = start_group(work_directory, f"sleep 1000 & echo $! > {work_directory}/child")
    child = read_pid(work_directory / "child")
    leader = group.process_group
    wait_until(lambda: not group.leader_alive())
    assert process_exists(child)
    assert_group_stopped(group, [leader, child], work_directory / "group.log")


def test_group_stop_when_leader_dies_on_term_but_descendant_ignores_it(work_directory, fast_drain):
    group = start_group(work_directory, f"(trap '' TERM; exec sleep 1000) & echo $! > {work_directory}/child; wait")
    child = read_pid(work_directory / "child")
    leader = group.process_group
    assert_group_stopped(group, [leader, child], work_directory / "group.log")


def test_group_stop_reports_undrained_group(work_directory, monkeypatch, fast_drain):
    group = start_group(work_directory, "exec sleep 1000")
    process_group = group.process_group
    monkeypatch.setattr(OwnedProcessGroup, "group_exists", lambda self: True)
    with pytest.raises(RuntimeError, match="did not drain"):
        group.stop()
    assert group.process_group == process_group
    assert str(work_directory / "group.log") not in open_file_paths()
    monkeypatch.undo()
    group.stop()


@pytest.fixture
def fake_binary(work_directory):
    binary = write_executable(work_directory / "fake-ydbd", "echo fake-ydbd-version\n")
    lock = {
        "binarySha256": file_sha256(binary),
        "versionOutputSha256": hashlib.sha256(b"fake-ydbd-version").hexdigest(),
    }
    return binary, lock


def forbid_processes(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("Unverified binary was executed")

    monkeypatch.setattr(public_ydb.subprocess, "run", fail)


def test_resolve_binary_accepts_delivered_and_exact_override(work_directory, fake_binary):
    binary, lock = fake_binary
    override = work_directory / "override-ydbd"
    override.symlink_to(binary)
    assert resolve_binary(lock, binary) == binary.resolve()
    assert resolve_binary(lock, work_directory / "missing", override) == binary.resolve()


def test_resolve_binary_rejects_appended_byte_before_execution(work_directory, fake_binary, monkeypatch):
    binary, lock = fake_binary
    tampered = work_directory / "tampered-ydbd"
    tampered.write_bytes(binary.read_bytes() + b"\n")
    tampered.chmod(0o700)
    forbid_processes(monkeypatch)
    with pytest.raises(RuntimeError, match="does not match"):
        resolve_binary(lock, binary, tampered)


def test_resolve_binary_rejects_invalid_override_before_execution(work_directory, fake_binary, monkeypatch):
    binary, lock = fake_binary
    forbid_processes(monkeypatch)
    with pytest.raises(FileNotFoundError):
        resolve_binary(lock, binary, work_directory / "absent-override")


def test_resolve_binary_rejects_version_mismatch(fake_binary):
    binary, lock = fake_binary
    with pytest.raises(RuntimeError, match="version"):
        resolve_binary(dict(lock, versionOutputSha256=hashlib.sha256(b"other-version").hexdigest()), binary)


class FakeRelease:
    VERSION = "fake-release-version-for-artifact-retention"

    def __init__(self, root):
        self.parameters = {}
        self.build_root = root / "build"
        self.retained_results = root / "retained-results"
        self.retained_output = self.retained_results / "testing_out_stuff"
        self.retained_output.mkdir(parents=True)
        self.temporary_space = root / "temporary-space"
        self.temporary_space.mkdir()
        self.ydbd, self.ydb = self._write_payload(root / "release" / "bin")
        self.archive = root / "ydbd.tar.gz"
        with tarfile.open(self.archive, "w:gz") as package:
            package.add(root / "release", arcname="release")
        self.lock = {
            "archiveUrl": self.archive.as_uri(),
            "archiveSha256": file_sha256(self.archive),
            "binarySha256": file_sha256(self.ydbd),
            "cliSha256": file_sha256(self.ydb),
            "versionOutputSha256": hashlib.sha256(self.VERSION.encode("utf-8")).hexdigest(),
        }

    def _write_payload(self, directory):
        directory.mkdir(parents=True)
        return (
            write_executable(directory / "ydbd", f"echo {self.VERSION}\n"),
            write_executable(directory / "ydb", f"echo {self.VERSION}-cli\n"),
        )

    def deliver(self):
        return self._write_payload(self.build_root / public_ydb.FIXTURE_SOURCE_DIRECTORY / "ydbd")

    def copies_in_retained_results(self):
        digests = {file_sha256(payload) for payload in (self.ydbd, self.ydb, self.archive)}
        return [path for path in self.retained_results.rglob("*") if path.is_file() and file_sha256(path) in digests]


def read_binary_identities():
    receipt = Path(yatest.common.output_path(public_ydb.BINARY_IDENTITY_RECEIPT))
    return [json.loads(line) for line in receipt.read_text().splitlines()]


@pytest.fixture
def fake_release(monkeypatch):
    with tempfile.TemporaryDirectory() as directory:
        release = FakeRelease(Path(directory).resolve())
        monkeypatch.setattr(tempfile, "tempdir", str(release.temporary_space))
        monkeypatch.setattr(yatest.common, "output_path", lambda path: str(release.retained_output / path))
        monkeypatch.setattr(
            yatest.common, "get_param", lambda name, default=None: release.parameters.get(name, default)
        )
        monkeypatch.setattr(yatest.common, "build_path", lambda path: str(release.build_root / path))
        monkeypatch.setattr(public_ydb, "_acquired_binaries", None, raising=False)
        monkeypatch.setattr(public_ydb, "_prepared_executables", {}, raising=False)
        yield release


def forbid_acquisition(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("Borrowed binaries must not trigger acquisition")

    monkeypatch.setattr(public_ydb, "acquire_locked_binary", fail)


def test_temporary_space_is_outside_retained_results():
    assert not Path(tempfile.gettempdir()).resolve().is_relative_to(Path(yatest.common.work_path()).resolve())


def test_acquired_binaries_stay_out_of_retained_results(fake_release):
    binary, cli = locate_fixture_binaries(fake_release.lock)
    assert fake_release.copies_in_retained_results() == []
    assert binary.is_relative_to(fake_release.temporary_space) and cli.is_relative_to(fake_release.temporary_space)
    assert file_sha256(binary) == fake_release.lock["binarySha256"]
    assert file_sha256(cli) == fake_release.lock["cliSha256"]
    assert read_binary_identities() == [
        {
            "archiveSha256": fake_release.lock["archiveSha256"],
            "binaryOrigin": "acquired",
            "binarySha256": fake_release.lock["binarySha256"],
            "cliOrigin": "acquired",
            "cliSha256": fake_release.lock["cliSha256"],
            "versionOutputSha256": fake_release.lock["versionOutputSha256"],
        }
    ]


def test_repeated_locate_keeps_the_acquired_binaries_in_place(fake_release):
    binary, cli = locate_fixture_binaries(fake_release.lock)
    inodes = (binary.stat().st_ino, cli.stat().st_ino)
    fake_release.archive.unlink()
    assert locate_fixture_binaries(fake_release.lock) == (binary, cli)
    assert (binary.stat().st_ino, cli.stat().st_ino) == inodes
    assert len(list(fake_release.temporary_space.iterdir())) == 1
    assert len(read_binary_identities()) == 2


def test_delivered_binaries_are_borrowed_in_place(fake_release, monkeypatch):
    ydbd, ydb = fake_release.deliver()
    forbid_acquisition(monkeypatch)
    assert locate_fixture_binaries(fake_release.lock) == (ydbd, ydb)
    assert ydbd.exists() and ydb.exists()
    assert list(fake_release.temporary_space.iterdir()) == []
    identity = read_binary_identities()[0]
    assert (identity["binaryOrigin"], identity["cliOrigin"]) == ("delivered", "delivered")
    assert "archiveSha256" not in identity


def test_overrides_are_borrowed_in_place(fake_release, monkeypatch):
    fake_release.parameters.update(YDB_BINARY=str(fake_release.ydbd), YDB_CLI=str(fake_release.ydb))
    forbid_acquisition(monkeypatch)
    assert locate_fixture_binaries(fake_release.lock) == (fake_release.ydbd, fake_release.ydb)
    assert fake_release.ydbd.exists() and fake_release.ydb.exists()
    assert list(fake_release.temporary_space.iterdir()) == []
    identity = read_binary_identities()[0]
    assert (identity["binaryOrigin"], identity["cliOrigin"]) == ("override", "override")


def test_partial_override_records_each_binary_origin(fake_release):
    fake_release.parameters.update(YDB_BINARY=str(fake_release.ydbd))
    binary, cli = locate_fixture_binaries(fake_release.lock)
    assert binary == fake_release.ydbd
    assert cli.is_relative_to(fake_release.temporary_space)
    identity = read_binary_identities()[0]
    assert (identity["binaryOrigin"], identity["cliOrigin"]) == ("override", "acquired")
    assert identity["archiveSha256"] == fake_release.lock["archiveSha256"]
    assert fake_release.copies_in_retained_results() == []


def test_mismatched_binary_records_no_identity(fake_release):
    with pytest.raises(RuntimeError, match="does not match"):
        locate_fixture_binaries(dict(fake_release.lock, binarySha256="0" * 64))
    assert not (fake_release.retained_output / public_ydb.BINARY_IDENTITY_RECEIPT).exists()


def make_read_only(*paths):
    for path in paths:
        path.chmod(0o444)


def file_states(*paths):
    return [(path.read_bytes(), path.stat().st_mode, path.stat().st_ino) for path in paths]


def run_fake_executable(path, *arguments):
    return subprocess.run(
        [str(path), *arguments], check=True, text=True, capture_output=True, stdin=subprocess.DEVNULL
    ).stdout.strip()


def assert_owned_copies_run(fake_release, sources, prepared):
    binary, cli = prepared
    assert run_fake_executable(binary, "-V") == fake_release.VERSION
    assert run_fake_executable(cli) == f"{fake_release.VERSION}-cli"
    for owned_copy, source in zip(prepared, sources):
        assert owned_copy.is_relative_to(fake_release.temporary_space)
        assert owned_copy.stat().st_ino != source.stat().st_ino
        assert stat.S_IMODE(owned_copy.stat().st_mode) == 0o700
        assert owned_copy.read_bytes() == source.read_bytes()
    assert fake_release.copies_in_retained_results() == []


def assert_repeated_locate_reuses_owned_copies(fake_release, prepared):
    inodes = [path.stat().st_ino for path in prepared]
    assert locate_fixture_binaries(fake_release.lock) == prepared
    assert [path.stat().st_ino for path in prepared] == inodes
    assert len(list(fake_release.temporary_space.iterdir())) == 2


def test_read_only_delivered_binaries_run_from_owned_copies(fake_release, monkeypatch):
    sources = fake_release.deliver()
    make_read_only(*sources)
    source_states = file_states(*sources)
    forbid_acquisition(monkeypatch)
    prepared = locate_fixture_binaries(fake_release.lock)
    assert_owned_copies_run(fake_release, sources, prepared)
    assert_repeated_locate_reuses_owned_copies(fake_release, prepared)
    assert file_states(*sources) == source_states
    origins = [(identity["binaryOrigin"], identity["cliOrigin"]) for identity in read_binary_identities()]
    assert origins == [("delivered", "delivered")] * 2
    assert not any("archiveSha256" in identity for identity in read_binary_identities())


def test_read_only_overrides_run_from_owned_copies(fake_release, monkeypatch):
    sources = (fake_release.ydbd, fake_release.ydb)
    fake_release.parameters.update(YDB_BINARY=str(fake_release.ydbd), YDB_CLI=str(fake_release.ydb))
    make_read_only(*sources)
    source_states = file_states(*sources)
    forbid_acquisition(monkeypatch)
    prepared = locate_fixture_binaries(fake_release.lock)
    assert_owned_copies_run(fake_release, sources, prepared)
    assert_repeated_locate_reuses_owned_copies(fake_release, prepared)
    assert file_states(*sources) == source_states
    origins = [(identity["binaryOrigin"], identity["cliOrigin"]) for identity in read_binary_identities()]
    assert origins == [("override", "override")] * 2


@pytest.mark.parametrize("resolve, digest, index", [(resolve_binary, "binarySha256", 0), (resolve_cli, "cliSha256", 1)])
def test_mismatched_read_only_source_fails_before_staging(fake_release, monkeypatch, resolve, digest, index):
    sources = fake_release.deliver()
    make_read_only(*sources)
    source = sources[index]
    forbid_processes(monkeypatch)
    with pytest.raises(RuntimeError, match="does not match"):
        resolve(dict(fake_release.lock, **{digest: "0" * 64}), source)
    assert list(fake_release.temporary_space.iterdir()) == []


def test_mismatched_read_only_override_records_no_identity(fake_release, monkeypatch):
    fake_release.parameters.update(YDB_BINARY=str(fake_release.ydbd), YDB_CLI=str(fake_release.ydb))
    make_read_only(fake_release.ydbd, fake_release.ydb)
    forbid_acquisition(monkeypatch)
    forbid_processes(monkeypatch)
    with pytest.raises(RuntimeError, match="does not match"):
        locate_fixture_binaries(dict(fake_release.lock, binarySha256="0" * 64))
    assert list(fake_release.temporary_space.iterdir()) == []
    assert not (fake_release.retained_output / public_ydb.BINARY_IDENTITY_RECEIPT).exists()


FAKE_CLI = """echo "ydb $*" >> "$FAKE_COMMANDS"
case "$*" in
  *"node config init"*) exit 0 ;;
  *"scheme mkdir"*) exit 0 ;;
  *"cluster bootstrap"*) exit ${FAKE_BOOTSTRAP_STATUS:-0} ;;
  *"scheme ls"*) exit 0 ;;
esac
exit 3
"""

FAKE_SERVER = """echo "ydbd $*" >> "$FAKE_COMMANDS"
if [ "$1" = server ]; then
  case "$FAKE_SERVER_MODE" in
    crash) exit 1 ;;
    *) trap '' TERM; sleep 1000 & echo $! >> "$FAKE_CHILDREN"; wait ;;
  esac
fi
exit ${FAKE_ADMIN_STATUS:-0}
"""


def create_fake_instance(work_directory, monkeypatch, **database):
    binary = write_executable(work_directory / "ydbd", FAKE_SERVER)
    cli = write_executable(work_directory / "ydb", FAKE_CLI)
    children = work_directory / "children"
    monkeypatch.setenv("FAKE_CHILDREN", str(children))
    monkeypatch.setenv("FAKE_COMMANDS", str(work_directory / "commands"))
    lock = load_lock()
    instance = PublicYdbInstance(
        work_directory / "instance",
        binary,
        lock,
        render_config(deferred_publish=False),
        ((1, 2, 3), (4, 5, 6)),
        work_directory / "absent-probe",
        cli,
        readiness_deadline_seconds=2,
        **database,
    )
    return instance, children


@pytest.fixture
def fake_instance(work_directory, monkeypatch, fast_drain):
    return create_fake_instance(work_directory, monkeypatch)


def read_fake_commands(work_directory):
    return (work_directory / "commands").read_text().splitlines()


def assert_instance_cleaned(instance, children):
    pids = [int(line) for line in children.read_text().split()] if children.exists() else []
    assert not any(process_exists(pid) for pid in pids)
    assert instance._owned_groups() == []
    assert not any(path.startswith(str(instance.directory)) for path in open_file_paths())


def test_instance_cleans_up_after_server_crash(fake_instance, monkeypatch):
    instance, children = fake_instance
    monkeypatch.setenv("FAKE_SERVER_MODE", "crash")
    monkeypatch.setenv("FAKE_BOOTSTRAP_STATUS", "1")
    with pytest.raises(TimeoutError, match="bootstrap"):
        instance.__enter__()
    assert_instance_cleaned(instance, children)


def test_instance_cleans_up_after_readiness_timeout(fake_instance, monkeypatch):
    instance, children = fake_instance
    monkeypatch.setenv("FAKE_BOOTSTRAP_STATUS", "1")
    with pytest.raises(TimeoutError, match="bootstrap"):
        instance.__enter__()
    assert children.read_text().split()
    assert_instance_cleaned(instance, children)


def test_instance_cleans_up_after_admin_failure(fake_instance, monkeypatch):
    instance, children = fake_instance
    monkeypatch.setenv("FAKE_ADMIN_STATUS", "1")
    with pytest.raises(subprocess.CalledProcessError):
        instance.__enter__()
    assert_instance_cleaned(instance, children)


def test_instance_cleans_up_both_nodes_after_probe_failure(fake_instance):
    instance, children = fake_instance
    with pytest.raises(FileNotFoundError):
        instance.__enter__()
    assert len(children.read_text().split()) == 2
    assert_instance_cleaned(instance, children)


def test_default_instance_bootstraps_root_database_without_parent(fake_instance, work_directory):
    instance, children = fake_instance
    with pytest.raises(FileNotFoundError):
        instance.__enter__()
    commands = read_fake_commands(work_directory)
    assert not any("scheme mkdir" in command for command in commands)
    assert "ydbd -s grpc://localhost:1 admin database /Root/db create ssd:1" in commands
    assert_instance_cleaned(instance, children)


def test_instance_propagates_nested_database(work_directory, monkeypatch, fast_drain):
    instance, children = create_fake_instance(work_directory, monkeypatch, database="/Root/topic-federation/account")
    assert instance.database == "/Root/topic-federation/account"
    with pytest.raises(FileNotFoundError):
        instance.__enter__()
    commands = read_fake_commands(work_directory)
    mkdir = "ydb -y -e grpc://localhost:1 -d /Root scheme mkdir topic-federation"
    create = "ydbd -s grpc://localhost:1 admin database /Root/topic-federation/account create ssd:1"
    readiness = "ydb -y -e grpc://localhost:4 -d /Root/topic-federation/account scheme ls"
    bootstrap = next(index for index, command in enumerate(commands) if "cluster bootstrap" in command)
    assert bootstrap < commands.index(mkdir) < commands.index(create) < commands.index(readiness)
    dynamic = next(command for command in commands if "--tenant" in command)
    assert "--tenant /Root/topic-federation/account --node-broker grpc://localhost:1" in dynamic
    assert_instance_cleaned(instance, children)


@pytest.fixture(scope="module")
def lock():
    return load_lock()


@pytest.fixture(scope="module")
def fixture_binaries(lock):
    return locate_fixture_binaries(lock)


def allocate_ports(port_manager):
    return tuple(tuple(port_manager.get_port() for _ in range(3)) for _ in range(2))


def run_public_instance(name, lock, fixture_binaries, deferred_publish, probe_modes, on_ready=None, **database):
    binary, cli = fixture_binaries
    with PortManager() as port_manager:
        ports = allocate_ports(port_manager)
        instance = PublicYdbInstance(
            Path(yatest.common.output_path(name)),
            binary,
            lock,
            render_config(deferred_publish),
            ports,
            yatest.common.binary_path(PROBE_PATH),
            cli,
            probe_modes=probe_modes,
            **database,
        )
        with instance:
            assert instance.driver is not None
            if on_ready is not None:
                on_ready(instance)
            instance.pause()
            instance.resume()
            results = list(instance.probe_results)
        for port in sum(ports, ()):
            assert_port_free(port)
    observed = Path(yatest.common.output_path("observed-capabilities.jsonl"))
    with observed.open("a") as stream:
        for result in results:
            stream.write(json.dumps(result, sort_keys=True) + "\n")
    return {result["mode"]: result for result in results}


def test_lock_identities_match_fixture_sources(lock, fixture_binaries):
    binary, cli = fixture_binaries
    assert lock["version"] == "26.3.1.16-rc"
    assert lock["platform"] == "linux-amd64"
    assert lock["archiveUrl"].endswith(f"/release/{lock['version']}/ydbd-{lock['version']}-linux-amd64.tar.gz")
    assert lock["configSha256"] == file_sha256(fixture_source_path("server_config.yaml"))
    assert file_sha256(binary) == lock["binarySha256"]
    assert file_sha256(cli) == lock["cliSha256"]
    retained_results = Path(yatest.common.work_path()).resolve()
    assert not binary.is_relative_to(retained_results) and not cli.is_relative_to(retained_results)
    identities = read_binary_identities()
    assert identities and all(identity["binarySha256"] == lock["binarySha256"] for identity in identities)
    assert all(identity["versionOutputSha256"] == lock["versionOutputSha256"] for identity in identities)


def test_regular_topic_capability(lock, fixture_binaries):
    results = run_public_instance("regular", lock, fixture_binaries, False, ("ordinary", "deferred-disabled"))
    for mode in ("ordinary", "deferred-disabled"):
        assert results[mode] == lock["capabilities"][mode]
    assert results["ordinary"]["operations"] == {"create": "SUCCESS", "write": ["RAW", "GZIP", "ZSTD"], "read": 3}
    assert results["deferred-disabled"]["operations"]["begin"] != "SUCCESS"


def test_deferred_publication_capability(lock, fixture_binaries):
    results = run_public_instance("deferred", lock, fixture_binaries, True, ("deferred-enabled",))
    assert results["deferred-enabled"] == lock["capabilities"]["deferred-enabled"]
    operations = results["deferred-enabled"]["operations"]
    assert operations["invisibleBeforePublish"] and operations["visibleAfterPublish"]
    assert results["deferred-enabled"]["configSha256"] == hashlib.sha256(render_config(True).encode()).hexdigest()


def test_nested_database_capability(lock, fixture_binaries):
    database = "/Root/topic-federation/account"
    listed_children = []

    def list_database(instance):
        assert instance.database == database
        directory = instance.driver.scheme_client.list_directory(database)
        listed_children.extend(child.name for child in directory.children)

    results = run_public_instance(
        "nested-database", lock, fixture_binaries, False, ("ordinary",), on_ready=list_database, database=database
    )
    assert results["ordinary"] == lock["capabilities"]["ordinary"]
    assert any(name.startswith("ytflow-fixture-probe-ordinary-") for name in listed_children)


AUTH_PROBE_PATH = "yt/yt/flow/extensions/ydb/test/auth_probe/ydb_auth_probe"
GRANTED_USER = "flowauth"
DENIED_USER = "flowdenied"
AUTH_PASSWORD = "YtflowSyntheticLogin2026"
WRONG_PASSWORD = "YtflowSyntheticWrong2026"
AUTH_TOPIC = "ytflow-auth-probe-topic"
AUTHENTICATION_FAILURES = {"CLIENT_UNAUTHENTICATED", "UNAUTHENTICATED"}
ACCESS_FAILURES = {"UNAUTHORIZED", "SCHEME_ERROR"}
AUTH_PROBE_TIMEOUT_SECONDS = 90


def provision_login_users(instance):
    import ydb

    settings = ydb.BaseRequestSettings().with_timeout(10)
    session = instance.driver.table_client.session().create(settings=settings)
    try:
        for user in (GRANTED_USER, DENIED_USER):
            session.execute_scheme(f"CREATE USER {user} PASSWORD '{AUTH_PASSWORD}';", settings)
        session.execute_scheme(f"GRANT 'ydb.generic.use' ON `{instance.database}` TO {GRANTED_USER};", settings)
    finally:
        session.delete(settings=settings)
    instance.driver.topic_client.create_topic(f"{instance.database}/{AUTH_TOPIC}")


def issue_login_token(instance, user):
    import ydb

    driver_config = ydb.DriverConfig(endpoint=f"grpc://{instance.endpoint}", database=instance.database)
    return ydb.StaticCredentials(driver_config, user, AUTH_PASSWORD).token


def driver_parameters(**parameters):
    return "{" + "".join(f'{name}="{value}";' for name, value in parameters.items()) + "}"


def run_auth_probe(instance, parameters, environment, injected=None):
    probe_environment = {name: value for name, value in os.environ.items() if not name.startswith("YDB")}
    probe_environment.update(environment)
    argv = [
        yatest.common.binary_path(AUTH_PROBE_PATH),
        instance.endpoint,
        instance.database,
        AUTH_TOPIC,
        parameters,
        *(injected or ()),
    ]
    completed = subprocess.run(
        argv,
        check=True,
        timeout=AUTH_PROBE_TIMEOUT_SECONDS,
        text=True,
        capture_output=True,
        stdin=subprocess.DEVNULL,
        env=probe_environment,
    )
    return json.loads(completed.stdout), completed.stdout + completed.stderr


def test_public_driver_authenticates_topic_operations(lock, fixture_binaries):
    outcomes = {}
    secrets = [AUTH_PASSWORD, WRONG_PASSWORD]

    def run_cases(instance):
        provision_login_users(instance)
        granted = issue_login_token(instance, GRANTED_USER)
        denied = issue_login_token(instance, DENIED_USER)
        assert granted and denied and granted != denied
        secrets.extend([granted, denied])
        login = driver_parameters(auth_mode="login", username=GRANTED_USER, password_env_var="FLOW_AUTH_PASSWORD")
        token = driver_parameters(auth_mode="token", token_env_var="FLOW_AUTH_TOKEN")
        auto_custom = driver_parameters(auth_mode="auto", token_env_var="FLOW_AUTH_TOKEN")
        cases = [
            ("login", login, {"FLOW_AUTH_PASSWORD": AUTH_PASSWORD, "YDB_TOKEN": denied}, None),
            (
                "login-denied-user",
                driver_parameters(auth_mode="login", username=DENIED_USER, password_env_var="FLOW_AUTH_PASSWORD"),
                {"FLOW_AUTH_PASSWORD": AUTH_PASSWORD, "YDB_TOKEN": granted},
                None,
            ),
            ("explicit-token", token, {"FLOW_AUTH_TOKEN": granted, "YDB_TOKEN": denied}, None),
            ("explicit-denied-token", token, {"FLOW_AUTH_TOKEN": denied, "YDB_TOKEN": granted}, None),
            ("auto-default-token", driver_parameters(auth_mode="auto"), {"YDB_TOKEN": granted}, None),
            ("auto-default-denied-token", driver_parameters(auth_mode="auto"), {"YDB_TOKEN": denied}, None),
            ("auto-custom-token", auto_custom, {"FLOW_AUTH_TOKEN": granted, "YDB_TOKEN": denied}, None),
            ("auto-custom-denied-token", auto_custom, {"FLOW_AUTH_TOKEN": denied, "YDB_TOKEN": granted}, None),
            (
                "injected-replaces-configured",
                login,
                {"FLOW_AUTH_PASSWORD": WRONG_PASSWORD, "FLOW_INJECTED_PASSWORD": AUTH_PASSWORD, "YDB_TOKEN": denied},
                (GRANTED_USER, "FLOW_INJECTED_PASSWORD"),
            ),
            ("empty-login-password", login, {"FLOW_AUTH_PASSWORD": "", "YDB_TOKEN": granted}, None),
            ("wrong-login", login, {"FLOW_AUTH_PASSWORD": WRONG_PASSWORD, "YDB_TOKEN": granted}, None),
            (
                "injected-wrong-password",
                login,
                {"FLOW_AUTH_PASSWORD": AUTH_PASSWORD, "FLOW_INJECTED_PASSWORD": WRONG_PASSWORD},
                (GRANTED_USER, "FLOW_INJECTED_PASSWORD"),
            ),
        ]
        for name, parameters, environment, injected in cases:
            outcomes[name] = run_auth_probe(instance, parameters, environment, injected)

    run_public_instance(
        "public-driver-authentication", lock, fixture_binaries, False, ("ordinary",), on_ready=run_cases
    )
    Path(yatest.common.output_path("public-driver-authentication.json")).write_text(
        json.dumps({name: result for name, (result, _) in outcomes.items()}, indent=2, sort_keys=True)
    )
    for name in ("login", "explicit-token", "auto-default-token", "auto-custom-token", "injected-replaces-configured"):
        assert outcomes[name][0] == {"stage": "describe", "status": "SUCCESS", "issues": "", "partitionCount": 1}, name
    for name in ("login-denied-user", "explicit-denied-token", "auto-default-denied-token", "auto-custom-denied-token"):
        assert outcomes[name][0]["stage"] == "describe", name
        assert outcomes[name][0]["status"] in ACCESS_FAILURES, (name, outcomes[name][0])
    for name in ("wrong-login", "injected-wrong-password"):
        assert outcomes[name][0]["stage"] == "describe", name
        assert outcomes[name][0]["status"] in AUTHENTICATION_FAILURES, (name, outcomes[name][0])
    empty_password = outcomes["empty-login-password"][0]
    assert empty_password["stage"] == "configure"
    assert "FLOW_AUTH_PASSWORD" in empty_password["error"]
    for name, (_, output) in outcomes.items():
        for secret in secrets:
            assert secret not in output, name
