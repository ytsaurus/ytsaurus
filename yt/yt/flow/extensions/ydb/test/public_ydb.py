import hashlib
import json
import os
import shutil
import signal
import subprocess
import tempfile
import time
from pathlib import Path, PurePosixPath

import yatest.common

from .acquire_server import acquire_locked_binary, file_sha256

FIXTURE_SOURCE_DIRECTORY = "yt/yt/flow/extensions/ydb/test"

DATABASE = "/Root/db"
CLUSTER_ROOT = "/Root"
LOGIN_USER = "root"
DEFERRED_PUBLISH_FEATURE_FLAGS = "  feature_flags:\n    enable_topic_deferred_publish: true\n"

READINESS_DEADLINE_SECONDS = 120
ADMIN_ATTEMPT_TIMEOUT_SECONDS = 5
DATABASE_CREATE_TIMEOUT_SECONDS = 60
PROBE_TIMEOUT_SECONDS = 120
GROUP_DRAIN_SECONDS = 10
VERSION_TIMEOUT_SECONDS = 15
BINARY_IDENTITY_RECEIPT = "public-ydb-binary-identity.jsonl"

_acquired_binaries = None
_prepared_executables = {}


def fixture_source_path(name):
    return Path(yatest.common.source_path(f"{FIXTURE_SOURCE_DIRECTORY}/{name}"))


def load_lock():
    return json.loads(fixture_source_path("server.lock.json").read_text())


def prepare_executable(source, expected_sha256, description):
    source = Path(source).resolve(strict=True)
    if file_sha256(source) != expected_sha256:
        raise RuntimeError(f"YDB fixture {description} does not match server.lock.json")
    if os.access(source, os.X_OK):
        return source
    prepared_key = (source, expected_sha256)
    if prepared_key not in _prepared_executables:
        owned_copy = Path(tempfile.mkdtemp(prefix="public-ydb-executable-")) / source.name
        descriptor = os.open(owned_copy, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o700)
        with source.open("rb") as input_stream, os.fdopen(descriptor, "wb") as output_stream:
            shutil.copyfileobj(input_stream, output_stream)
        owned_copy.chmod(0o700)
        if file_sha256(owned_copy) != expected_sha256:
            raise RuntimeError(f"Prepared YDB fixture {description} does not match server.lock.json")
        _prepared_executables[prepared_key] = owned_copy
    return _prepared_executables[prepared_key]


def resolve_binary(lock, delivered_path, override_path=None):
    binary = prepare_executable(override_path or delivered_path, lock["binarySha256"], "binary")
    version = subprocess.run(
        [str(binary), "-V"],
        check=True,
        text=True,
        capture_output=True,
        timeout=VERSION_TIMEOUT_SECONDS,
        stdin=subprocess.DEVNULL,
    )
    if hashlib.sha256(version.stdout.strip().encode("utf-8")).hexdigest() != lock["versionOutputSha256"]:
        raise RuntimeError("YDB fixture version does not match server.lock.json")
    return binary


def resolve_cli(lock, delivered_path, override_path=None):
    return prepare_executable(override_path or delivered_path, lock["cliSha256"], "CLI")


def acquire_binaries_outside_test_output(lock):
    global _acquired_binaries
    if _acquired_binaries is None:
        _acquired_binaries = acquire_locked_binary(lock, tempfile.mkdtemp(prefix="public-ydb-archive-"))
    return _acquired_binaries


def record_binary_identity(lock, binary_origin, cli_origin):
    identity = {
        "binaryOrigin": binary_origin,
        "binarySha256": lock["binarySha256"],
        "cliOrigin": cli_origin,
        "cliSha256": lock["cliSha256"],
        "versionOutputSha256": lock["versionOutputSha256"],
    }
    if "acquired" in (binary_origin, cli_origin):
        identity["archiveSha256"] = lock["archiveSha256"]
    with Path(yatest.common.output_path(BINARY_IDENTITY_RECEIPT)).open("a") as stream:
        stream.write(json.dumps(identity, sort_keys=True) + "\n")


def locate_fixture_binaries(lock):
    binary_override = yatest.common.get_param("YDB_BINARY")
    cli_override = yatest.common.get_param("YDB_CLI")
    default_binary = Path(yatest.common.build_path(f"{FIXTURE_SOURCE_DIRECTORY}/ydbd/ydbd"))
    default_cli = Path(yatest.common.build_path(f"{FIXTURE_SOURCE_DIRECTORY}/ydbd/ydb"))
    default_origin = "delivered"
    if not (binary_override and cli_override) and not (default_binary.exists() and default_cli.exists()):
        default_binary, default_cli = acquire_binaries_outside_test_output(lock)
        default_origin = "acquired"
    binary = resolve_binary(lock, default_binary, binary_override)
    cli = resolve_cli(lock, default_cli, cli_override)
    record_binary_identity(
        lock,
        "override" if binary_override else default_origin,
        "override" if cli_override else default_origin,
    )
    return binary, cli


def render_config(deferred_publish):
    config = fixture_source_path("server_config.yaml").read_text()
    if deferred_publish:
        config += DEFERRED_PUBLISH_FEATURE_FLAGS
    return config


class OwnedProcessGroup:
    def __init__(self, argv, cwd, log_path, env=None):
        self.argv = [str(argument) for argument in argv]
        self.cwd = cwd
        self.log_path = Path(log_path)
        self.env = env
        self._process = None
        self._process_group = None
        self._log = None

    @property
    def process_group(self):
        return self._process_group

    def start(self):
        self._log = self.log_path.open("ab")
        self._process = subprocess.Popen(
            self.argv,
            cwd=self.cwd,
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=self._log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self._process_group = self._process.pid

    def leader_alive(self):
        return self._process is not None and self._process.poll() is None

    def signal(self, value):
        if self._process_group is None:
            return
        try:
            os.killpg(self._process_group, value)
        except ProcessLookupError:
            pass

    def group_exists(self):
        if self._process is not None:
            self._process.poll()
        try:
            os.killpg(self._process_group, 0)
            return True
        except ProcessLookupError:
            return False

    def _wait_for_drain(self):
        deadline = time.monotonic() + GROUP_DRAIN_SECONDS
        while self.group_exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        return not self.group_exists()

    def stop(self):
        try:
            if self._process_group is not None:
                self.signal(signal.SIGCONT)
                self.signal(signal.SIGTERM)
                if not self._wait_for_drain():
                    self.signal(signal.SIGKILL)
                    if not self._wait_for_drain():
                        raise RuntimeError(f"Owned server process group {self._process_group} did not drain")
            if self._process is not None:
                self._process.wait(timeout=1)
            self._process = None
            self._process_group = None
        finally:
            if self._log is not None:
                self._log.close()
                self._log = None


def run_until_success(argv, log, deadline_seconds, attempt_timeout_seconds, description):
    deadline = time.monotonic() + deadline_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"{description} did not succeed within {deadline_seconds} seconds")
        try:
            subprocess.run(
                argv,
                check=True,
                timeout=min(attempt_timeout_seconds, remaining),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            time.sleep(min(1, max(0, deadline - time.monotonic())))


class PublicYdbInstance:
    def __init__(
        self,
        directory,
        binary,
        lock,
        config,
        ports,
        probe,
        cli,
        probe_modes=("ordinary",),
        readiness_deadline_seconds=READINESS_DEADLINE_SECONDS,
        database=DATABASE,
    ):
        self.directory = Path(directory)
        self.binary = Path(binary)
        self.cli = Path(cli)
        self.lock = lock
        self.config = config
        self.static_ports, self.dynamic_ports = ports
        self.probe = Path(probe)
        self.probe_modes = tuple(probe_modes)
        self.readiness_deadline_seconds = readiness_deadline_seconds
        self.endpoint = f"localhost:{self.dynamic_ports[0]}"
        self.database = database
        self.driver = None
        self.probe_results = []
        self._static_node = None
        self._dynamic_node = None
        self._log = None

    def _startup_argv(self, name, ports):
        values = {
            "ydbd": self.binary,
            "config_dir": self.directory / f"{name}-config",
            "grpc_port": ports[0],
            "interconnect_port": ports[1],
            "monitoring_port": ports[2],
            "static_grpc_port": self.static_ports[0],
            "database": self.database,
        }
        return [argument.format(**values) for argument in self.lock["startupArgv"][name]]

    def _admin_argv(self, *arguments, database=CLUSTER_ROOT, grpc_port=None):
        endpoint = f"grpc://localhost:{grpc_port or self.static_ports[0]}"
        return [str(self.cli), "-y", "-e", endpoint, "-d", database, *arguments]

    def _run_admin(self, argv, timeout):
        subprocess.run(
            argv, check=True, timeout=timeout, stdin=subprocess.DEVNULL, stdout=self._log, stderr=subprocess.STDOUT
        )

    def _probe_environment(self):
        environment = dict(os.environ)
        environment["YDB_USER"] = LOGIN_USER
        environment.pop("YDB_PASSWORD", None)
        return environment

    def _run_probe(self, mode):
        completed = subprocess.run(
            [str(self.probe), self.endpoint, self.database, mode],
            check=True,
            timeout=PROBE_TIMEOUT_SECONDS,
            text=True,
            capture_output=True,
            stdin=subprocess.DEVNULL,
            env=self._probe_environment(),
        )
        result = json.loads(completed.stdout)
        result["binarySha256"] = self.lock["binarySha256"]
        result["configSha256"] = hashlib.sha256(self.config.encode()).hexdigest()
        return result

    def _create_driver(self):
        import ydb

        driver_config = ydb.DriverConfig(endpoint=f"grpc://{self.endpoint}", database=self.database)
        driver_config.credentials = ydb.StaticCredentials(driver_config, LOGIN_USER)
        driver = ydb.Driver(driver_config)
        try:
            driver.wait(timeout=30)
        except BaseException:
            driver.stop()
            raise
        return driver

    def __enter__(self):
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            self._log = (self.directory / "fixture.log").open("ab")
            config_path = self.directory / "config.yaml"
            config_path.write_text(self.config.replace("${interconnect_port}", str(self.static_ports[1])))
            self._run_admin(
                [
                    str(self.cli),
                    "-y",
                    "admin",
                    "node",
                    "config",
                    "init",
                    "--config-dir",
                    str(self.directory / "static-config"),
                    "--from-config",
                    str(config_path),
                ],
                timeout=ADMIN_ATTEMPT_TIMEOUT_SECONDS * 6,
            )
            (self.directory / "dynamic-config").mkdir(exist_ok=True)
            self._static_node = OwnedProcessGroup(
                self._startup_argv("static", self.static_ports), self.directory, self.directory / "static.log"
            )
            self._static_node.start()
            run_until_success(
                self._admin_argv("admin", "cluster", "bootstrap", "--uuid", "ytflow-public-ydb-fixture"),
                self._log,
                self.readiness_deadline_seconds,
                ADMIN_ATTEMPT_TIMEOUT_SECONDS,
                "Public YDB cluster bootstrap",
            )
            self._create_database_parent()
            self._run_admin(
                [
                    str(self.binary),
                    "-s",
                    f"grpc://localhost:{self.static_ports[0]}",
                    "admin",
                    "database",
                    self.database,
                    "create",
                    "ssd:1",
                ],
                timeout=DATABASE_CREATE_TIMEOUT_SECONDS,
            )
            self._dynamic_node = OwnedProcessGroup(
                self._startup_argv("dynamic", self.dynamic_ports), self.directory, self.directory / "dynamic.log"
            )
            self._dynamic_node.start()
            run_until_success(
                self._admin_argv("scheme", "ls", database=self.database, grpc_port=self.dynamic_ports[0]),
                self._log,
                self.readiness_deadline_seconds,
                ADMIN_ATTEMPT_TIMEOUT_SECONDS,
                "Public YDB database readiness",
            )
            self.probe_results = [self._run_probe(mode) for mode in self.probe_modes]
            self.driver = self._create_driver()
            return self
        except BaseException:
            self.stop()
            raise

    def _create_database_parent(self):
        parent = PurePosixPath(self.database).parent.relative_to(CLUSTER_ROOT)
        if parent != PurePosixPath("."):
            self._run_admin(self._admin_argv("scheme", "mkdir", str(parent)), timeout=DATABASE_CREATE_TIMEOUT_SECONDS)

    def __exit__(self, exception_type, exception, traceback):
        self.stop()

    def _owned_groups(self):
        return [node for node in (self._dynamic_node, self._static_node) if node is not None]

    def stop(self):
        try:
            if self.driver is not None:
                driver, self.driver = self.driver, None
                driver.stop(timeout=10)
        finally:
            self._stop_owned_groups()

    def _stop_owned_groups(self):
        try:
            drain_failures = []
            for node in self._owned_groups():
                try:
                    node.stop()
                except RuntimeError as failure:
                    drain_failures.append(failure)
            if drain_failures:
                raise drain_failures[0]
            self._dynamic_node = None
            self._static_node = None
        finally:
            if self._log is not None:
                self._log.close()
                self._log = None

    def pause(self):
        for node in self._owned_groups():
            node.signal(signal.SIGSTOP)

    def resume(self):
        for node in self._owned_groups():
            node.signal(signal.SIGCONT)
