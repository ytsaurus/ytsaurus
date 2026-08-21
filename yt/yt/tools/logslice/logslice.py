#!/usr/bin/env python3
"""Remote log grepping via the `logslice` tool.

Usage:
    logslice.py remote_machine_name [--type type] [-l logslice]
                [-t start_time] [-e end_time] -- grep_args...

The script copies a (release-built) `logslice` binary to the remote machine,
discovers the rotated log files of the requested type, narrows them down to the
ones that overlap the requested [start_time, end_time] window (using the file
rotation hints plus a binary search driven by `logslice --info`), and finally
runs `logslice` on each selected file, forwarding the grep arguments.

Besides the live ``logs`` directory the script also looks into the master log
archive (``/yt/master-logs-archive/<YYYY-MM-DD>/...``, see ``--archive-dir``),
where old rotated files are moved under per-day subdirectories. The archive is
only consulted when a time window is given, and only for the day subdirectories
overlapping that window.

All times are interpreted exactly as `logslice` interprets them (server local
time); the precise filtering is always delegated to `logslice` itself, so the
in-script time handling only needs to be good enough to pick the right files.
"""

import argparse
import hashlib
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
from datetime import datetime


REMOTE_DIR = "/tmp"
REMOTE_BIN = REMOTE_DIR + "/logslice"
REMOTE_LOGS_DIR = "logs"

# Old rotated master logs are moved here, under per-day subdirectories named
# "YYYY-MM-DD", e.g. /yt/master-logs-archive/2026-06-19/master-klg0-0941.debug.log.2026-06-19_07-15.zst
ARCHIVE_DIR_DEFAULT = "/yt/master-logs-archive"

# A debug logslice is hundreds of MiB; a release one is well under this. Anything
# below the limit is assumed to be a usable release build.
RELEASE_SIZE_LIMIT = 3 * 1024 * 1024
CONTROL_PERSIST_DEFAULT = 3600
CONNECT_TIMEOUT_DEFAULT = 30
CONTROL_PATH_ENV = "LOGSLICE_SSH_CONTROL_PATH"
CONTROL_PERSIST_ENV = "LOGSLICE_SSH_CONTROL_PERSIST"
GLOBAL_NO_MATCH_EXIT = 3
OPERATIONAL_FAILURE_EXIT = 2
LOG_TYPES = ("debug", "info", "error")

# Path of this script inside Arcadia, used both to locate the binary and to
# recover the Arcadia root when the script is run from an arbitrary directory.
ARCADIA_SUFFIX = os.path.join("yt", "yt", "tools", "logslice")
BUILD_TARGET = "yt/yt/tools/logslice/bin"


def eprint(*args):
    print(*args, file=sys.stderr)


def describe_exit_code(returncode):
    """Human-readable explanation of a remote logslice exit code."""
    if returncode > 128:
        signal = returncode - 128
        hint = " (likely out of memory)" if signal == 9 else ""
        return "killed by signal {}{}, exit code {}".format(
            signal, hint, returncode)
    return "exit code {}".format(returncode)


def parse_log_types(value):
    """Parse one severity, a comma-separated set, or ``all``."""
    raw = list(LOG_TYPES) if value == "all" else value.split(",")
    result = []
    for item in raw:
        item = item.strip()
        if item not in LOG_TYPES:
            raise ValueError(
                "unknown log type {!r}; expected debug, info, error, a "
                "comma-separated combination, or all".format(item))
        if item not in result:
            result.append(item)
    if not result:
        raise ValueError("at least one log type is required")
    return result


LOG_RECORD_TIME_RE = re.compile(
    r"^(?P<stamp>\d{4}-\d{2}-\d{2}[ T]"
    r"\d{2}:\d{2}:\d{2}[,.]\d+)")


def _timestamped_records(text, source_index):
    records = []
    current = None
    ordinal = 0
    for line in text.splitlines(True):
        match = LOG_RECORD_TIME_RE.match(line)
        if match:
            if current is not None:
                records.append(current)
            stamp = match.group("stamp").replace(",", ".").replace(" ", "T")
            current = [stamp, source_index, ordinal, line]
            ordinal += 1
        elif current is None:
            current = ["", source_index, ordinal, line]
            ordinal += 1
        else:
            current[3] += line
    if current is not None:
        records.append(current)
    return records


def merge_timestamped_outputs(outputs):
    """Merge severity/file outputs by record timestamp, preserving continuations."""
    records = []
    for source_index, text in enumerate(outputs):
        records.extend(_timestamped_records(text, source_index))
    records.sort(key=lambda record: (record[0], record[1], record[2]))
    return "".join(record[3] for record in records)


def classify_slice_result(returncode, stdout, stderr):
    """Distinguish grep no-match from decompression/SSH/tool failure."""
    if returncode >= 2 or (returncode != 0 and stderr.strip()):
        return "failed"
    if returncode == 1:
        return "no_match"
    if stdout:
        return "matched"
    if returncode in (0, 1):
        return "no_match"
    return "failed"


def slice_exit_code(results):
    if any(item["status"] == "failed" for item in results):
        return OPERATIONAL_FAILURE_EXIT
    if not any(item["status"] == "matched" for item in results):
        return GLOBAL_NO_MATCH_EXIT
    return 0


def slice_failure_class(returncode, stderr):
    text = (stderr or "").lower()
    if "permission denied" in text and "publickey" in text:
        return "authentication"
    if ("connection timed out" in text or "connection reset" in text
            or "broken pipe" in text or "could not resolve hostname" in text):
        return "transport"
    if "decompress" in text or "zstd" in text or "gzip" in text:
        return "decompression"
    if returncode > 128:
        return "signal"
    return "command"


def preflight_failure_class(error):
    """Classify failures that happen before remote log discovery starts.

    Keep this deliberately narrow: only authentication failures receive the
    structured ``authentication_unavailable`` outcome. Transport and other SSH
    failures retain the existing diagnostic instead of being relabelled as an
    authentication problem.
    """
    text = str(error).lower()
    authentication_markers = (
        "ssh_auth_sock does not exist",
        "permission denied (publickey",
        "agent refused operation",
        "no identities available",
    )
    if any(marker in text for marker in authentication_markers):
        return "authentication_unavailable"
    return None


def requested_component(host, override):
    """Return the component intended by the caller before remote discovery."""
    if override:
        return override
    _, component = infer_host_component(host)
    return component or "unresolved"


def report_authentication_preflight(host, component, start, end):
    """Emit a bounded result for an SSH authentication failure.

    No remote directory has been listed at this point, so ``files=0`` and
    ``rotations_inspected=0`` are evidence boundaries rather than estimates.
    The underlying SSH diagnostic is intentionally not copied into the record:
    the outcome carries all useful classification without exposing agent or key
    details.
    """
    eprint(
        "preflight_result status=authentication_unavailable host={} "
        "component={} window_start={} window_end={} rotations_inspected=0"
        .format(host, component, start or "open", end or "open"))
    eprint(
        "summary timezone=unknown window_start={} window_end={} files=0 "
        "matches=0 failure_class=authentication_unavailable exit_code={}"
        .format(start or "open", end or "open", OPERATIONAL_FAILURE_EXIT))


def validate_debug_window(log_types, start_time, end_time, allow_broad=False):
    """Reject unbounded or >60 s debug scans unless explicitly overridden."""
    if "debug" not in log_types or allow_broad:
        return
    if start_time is None or end_time is None:
        raise ValueError(
            "debug logs require a bounded -t/-e window; find an exact error "
            "or Monium timestamp first, or pass --allow-broad-debug")
    seconds = (end_time - start_time).total_seconds()
    if seconds < 0:
        raise ValueError("the end of the log window precedes its start")
    if seconds > 60:
        raise ValueError(
            "debug window is {:.3f}s; narrow it to at most 60s around an "
            "observed transition, or pass --allow-broad-debug".format(seconds))


########################################################################
# Locating / building the logslice binary.
########################################################################

def find_arcadia_root():
    """Best-effort search for the Arcadia root.

    Tries, in order: walking up from this script, walking up from the current
    directory, and finally treating the current directory as the root. Returns
    the root path or None.
    """
    candidates = [os.path.dirname(os.path.realpath(__file__)), os.getcwd()]
    for start in candidates:
        path = start
        while True:
            if os.path.exists(os.path.join(path, ".arcadia.root")) or \
                    os.path.exists(os.path.join(path, "ya")):
                return path
            parent = os.path.dirname(path)
            if parent == path:
                break
            path = parent
    if os.path.isdir(os.path.join(os.getcwd(), ARCADIA_SUFFIX)):
        return os.getcwd()
    return None


def resolve_logslice(explicit_path):
    """Returns a path to a local release-built logslice binary, building it if
    necessary.

    With -l we trust the caller. Otherwise we locate the Arcadia checkout and use
    the existing binary if it is present and small enough to be a release build
    (so it copies to the remote machine quickly); a debug build, or a missing
    one, triggers ``ya make -r``."""
    if explicit_path:
        if not os.path.isfile(explicit_path):
            sys.exit("logslice binary not found at {}".format(explicit_path))
        return explicit_path

    arcadia_root = find_arcadia_root()
    if arcadia_root is None:
        sys.exit(
            "Could not locate the Arcadia root or the logslice directory.\n"
            "Run this script from inside Arcadia or pass the binary via -l.")

    bin_path = os.path.join(arcadia_root, ARCADIA_SUFFIX, "bin", "logslice")

    need_build = True
    if os.path.isfile(bin_path):
        # bin_path is usually a symlink into the build cache; follow it.
        size = os.path.getsize(os.path.realpath(bin_path))
        if size <= RELEASE_SIZE_LIMIT:
            need_build = False
        else:
            eprint("Existing logslice is too large to be a release build "
                   "({:.0f} MiB); rebuilding with -r.".format(size / 1024 / 1024))

    if need_build:
        ya = os.path.join(arcadia_root, "ya")
        eprint("Building release logslice: ya make -r {}".format(BUILD_TARGET))
        subprocess.check_call([ya, "make", "-r", BUILD_TARGET], cwd=arcadia_root)
        if not os.path.isfile(bin_path):
            sys.exit("Build finished but {} is missing.".format(bin_path))

    return bin_path


########################################################################
# SSH helpers (connection multiplexed so we authenticate once).
########################################################################

class PipelineResult:
    def __init__(self, returncode, operational_returncode, stdout, stderr):
        self.returncode = returncode
        self.operational_returncode = operational_returncode
        self.stdout = stdout
        self.stderr = stderr


class Ssh:
    # Only these programs may start a remote pipeline stage. Validation lives here,
    # right next to the ssh invocation, so there is no path that reaches the remote
    # shell without passing the whitelist; stage arguments are always shlex-quoted.
    # Do NOT add "awk" in this list (because of `system()` call).
    PIPELINE_WHITELIST = frozenset(["grep", "wc", "cut", "sed", "head", "tail"])

    def __init__(
            self,
            host,
            verbose=False,
            control_socket=None,
            control_persist=None,
            connect_timeout=CONNECT_TIMEOUT_DEFAULT):
        self.host = host
        self.verbose = verbose
        if control_persist is None:
            control_persist = int(os.environ.get(
                CONTROL_PERSIST_ENV, CONTROL_PERSIST_DEFAULT))
        self._control_persist = control_persist
        self._control_path = control_socket \
            or os.environ.get(CONTROL_PATH_ENV) \
            or os.path.join(tempfile.gettempdir(), "logslice_ssh_%r@%h:%p")
        self._base_opts = [
            "-o", "ControlMaster=auto",
            "-o", "ControlPath=" + self._control_path,
            "-o", "ControlPersist={}".format(control_persist),
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout={}".format(connect_timeout),
        ]

    def connect(self):
        """Establishes the master connection (this is where the key touch
        happens, exactly once).

        ssh automatically reuses a live caller-provided control socket. Running
        the probe even when its path exists also detects stale sockets instead
        of assuming that any filesystem entry is a usable master connection.
        """
        eprint("Connecting to {} (you may need to touch your security key)..."
               .format(self.host))
        self.run(["true"])

    def _remote_command(self, argv):
        return " ".join(shlex.quote(token) for token in argv)

    def run(
            self,
            argv,
            capture=True,
            check=True,
            warn_on_error=False,
            retries=0,
            retry_delay=1.0,
            retry_on_empty=False):
        """Runs argv on the remote host. Returns stdout (text) when capture is
        set, otherwise streams stdout/stderr straight through.

        With check=False a non-zero exit is not fatal: the (possibly empty)
        stdout is returned instead of aborting. Used for optional probes such as
        listing the log archive, which simply does not exist on most hosts —
        that benign "No such file or directory" stays silent by default. Pass
        warn_on_error=True to surface the stderr of a non-fatal failure (e.g.
        md5sum, where a swallowed error would look like "file absent" and cause
        a needless re-copy).

        Captured commands may be retried. retry_on_empty additionally treats a
        successful command with empty stdout as retryable; this is useful for
        probes such as `logslice --info`, where empty output cannot be useful.
        """
        cmd = ["ssh"] + self._base_opts + [self.host, self._remote_command(argv)]
        if not capture:
            if retries or retry_on_empty:
                raise ValueError("Retries require capture=True.")
            if self.verbose:
                eprint("Executing: {}".format(
                    " ".join(shlex.quote(c) for c in cmd)))
            return subprocess.run(cmd).returncode

        result = None
        for attempt in range(retries + 1):
            if self.verbose:
                suffix = " (attempt {})".format(attempt + 1) if attempt else ""
                eprint("Executing{}: {}".format(
                    suffix, " ".join(shlex.quote(c) for c in cmd)))
            result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True)
            failed = result.returncode != 0
            empty = retry_on_empty and not result.stdout.strip()
            if not failed and not empty:
                return result.stdout
            if attempt < retries:
                time.sleep(retry_delay)

        assert result is not None
        attempts = retries + 1
        if result.returncode != 0:
            error = result.stderr.strip() or "no stderr"
            if check:
                sys.exit("ssh {} failed after {} attempt(s): {}".format(
                    " ".join(argv), attempts, error))
            if warn_on_error:
                eprint("Warning: ssh {} exited {} after {} attempt(s) "
                       "(non-fatal): {}".format(
                           " ".join(argv), result.returncode, attempts, error))
        elif retry_on_empty and warn_on_error:
            eprint("Warning: ssh {} returned empty output after {} attempt(s)."
                   .format(" ".join(argv), attempts))
        return result.stdout

    @classmethod
    def validate_pipeline(cls, stages):
        """Raises ValueError unless every stage is non-empty and starts with a
        whitelisted command."""
        for stage in stages:
            if not stage:
                raise ValueError("Empty pipeline stage.")
            if stage[0] not in cls.PIPELINE_WHITELIST:
                raise ValueError("Command {!r} is not whitelisted; allowed: {}"
                                 .format(stage[0],
                                         ", ".join(sorted(cls.PIPELINE_WHITELIST))))

    def run_pipeline(self, head_argv, stages, capture=False):
        """Runs head_argv piped through stages on the remote host. Re-validates the
        whitelist immediately before invocation and shlex-quotes every token, so
        nothing can break out into shell syntax. The remote bash wrapper reports
        every stage's status and exits with the head (logslice) status; therefore a
        grep with no matches is not misreported as a logslice failure. Returns
        stdout when capture is set, otherwise the effective pipeline exit code."""
        result = self.run_pipeline_result(head_argv, stages, capture=capture)
        if capture:
            if result.operational_returncode != 0:
                detail = result.stderr or "exit code {}".format(
                    result.operational_returncode)
                sys.exit("ssh command failed: {}".format(detail))
            return result.stdout
        return result.operational_returncode

    def run_pipeline_result(self, head_argv, stages, capture=True):
        """Run a validated pipeline and retain its output classification.

        ``returncode`` is 1 when any grep stage has no matches, even if a later
        presentation stage such as ``wc`` succeeds. ``operational_returncode``
        preserves the legacy behavior where grep no-match is not a tool failure.
        """
        self.validate_pipeline(stages)
        pipeline = self._remote_command(head_argv)
        for stage in stages:
            pipeline += " | " + self._remote_command(stage)
        script = (
            pipeline
            + "; statuses=(\"${PIPESTATUS[@]}\"); "
            + "printf '__LOGSLICE_PIPESTATUS__:%s\\n' \"${statuses[*]}\" >&2; "
            + "exit \"${statuses[0]}\""
        )
        remote_command = "bash -c " + shlex.quote(script)
        cmd = ["ssh"] + self._base_opts + [self.host, remote_command]
        if self.verbose:
            eprint("Executing: {}".format(" ".join(shlex.quote(c) for c in cmd)))
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE,
            text=True,
        )
        statuses = []
        stderr = []
        marker = "__LOGSLICE_PIPESTATUS__:"
        for line in result.stderr.splitlines():
            if line.startswith(marker):
                try:
                    statuses = [int(value) for value in line[len(marker):].split()]
                except ValueError:
                    stderr.append(line)
            else:
                stderr.append(line)
        if stderr:
            eprint("\n".join(stderr))

        operational_returncode = result.returncode
        grep_no_match = False
        if statuses:
            operational_returncode = statuses[0]
            for stage, status in zip(stages, statuses[1:]):
                if stage[0] == "grep" and status == 1:
                    grep_no_match = True
                elif status != 0:
                    operational_returncode = status
                    break

        returncode = operational_returncode
        if returncode == 0 and grep_no_match:
            returncode = 1
        return PipelineResult(
            returncode=returncode,
            operational_returncode=operational_returncode,
            stdout=result.stdout or "",
            stderr="\n".join(stderr),
        )

    def remote_md5(self, remote_path):
        """Returns the md5 hex of an executable remote file, or None otherwise.

        A missing or non-executable binary is a normal upload case and stays
        silent. Connection, permission, and command failures remain visible.
        Unparseable successful output is also reported before re-uploading.
        """
        command = 'if [ -x "$1" ]; then md5sum -- "$1"; fi'
        out = self.run(
            ["sh", "-c", command, "logslice-md5", remote_path],
            check=False,
            capture=True,
            warn_on_error=True)
        if not out or not out.strip():
            return None
        token = out.strip().split()[0]
        if re.fullmatch(r"[0-9a-f]{32}", token):
            return token
        eprint("Warning: md5sum {} returned unparseable output {!r}; "
               "treating remote binary as absent (will re-copy).".format(
                   remote_path, out.strip()))
        return None

    def copy_binary(self, local_path, remote_path):
        """Copy the local binary to the remote host, reusing the ssh control
        socket (NOT a separate scp connection).

        scp opens its own connection even with ControlMaster=auto, which on
        Yubikey/TouchID-secured hosts demands a fresh key touch and fails
        non-interactively. Piping the file through `ssh ... cat > remote_file`
        reuses the master socket already opened by connect(), so no extra
        touch is needed.

        Skipped entirely when the remote already has the same binary (md5
        match) — common when the same host is queried repeatedly in a session,
        and the copy would just demand another key touch for nothing.
        """
        local_md5 = _file_md5(local_path)
        remote_md5 = self.remote_md5(remote_path)
        if remote_md5 == local_md5:
            if self.verbose:
                eprint("Remote {} already matches local binary (md5={}); "
                       "skipping copy.".format(remote_path, local_md5[:8]))
            return
        self._copy_via_ssh(local_path, remote_path)

    def _copy_via_ssh(self, local_path, remote_path):
        """Upload through ssh and atomically replace the remote executable.

        Writing directly to remote_path could leave a truncated executable when
        two logslice invocations overlap or an upload is interrupted. A
        per-remote-shell temporary file keeps the old binary usable until the
        complete replacement is executable and ready to rename.
        """
        quoted_remote_path = shlex.quote(remote_path)
        remote_command = (
            "tmp={}.upload.$$; "
            "trap 'rm -f -- \"$tmp\"' 0; "
            "cat > \"$tmp\" && chmod +x \"$tmp\" && "
            "mv -f -- \"$tmp\" {}"
        ).format(quoted_remote_path, quoted_remote_path)
        cmd = ["ssh"] + self._base_opts + [self.host, remote_command]
        if self.verbose:
            eprint("Copying binary via: {}".format(
                " ".join(shlex.quote(c) for c in cmd)))
        with open(local_path, "rb") as f:
            result = subprocess.run(cmd, stdin=f, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        if result.returncode != 0:
            err = result.stderr.decode("utf-8", "replace").strip()
            # A copy failure on a Yubikey/TouchID-secured host is almost always
            # the control socket being gone / a needed key touch. Hint at the
            # cause + the escape hatch instead of a bare "copy failed: lost
            # connection" that gives no clue how to recover.
            hint = ""
            if ("lost connection" in err or "Broken pipe" in err
                    or "timed out" in err):
                hint = ("\n  This usually means the ssh control socket expired "
                        "(ControlPersist) or the host needs a Yubikey/TouchID "
                        "touch. Re-run, or open a long-lived socket once with: "
                        "ssh -M -S <path> -o ControlPersist={} {} true "
                        "and pass --control-socket <path>.".format(
                            self._control_persist, self.host))
            sys.exit("copy failed: {}{}".format(err, hint))


def _file_md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


########################################################################
# Log file name parsing.
########################################################################

TIMESTAMP_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})$")
SEQUENCE_RE = re.compile(r"^\d+$")

CHANNEL_BY_TYPE = {"debug": "debug", "error": "error", "info": ""}

# Sidecar agents (e.g. timbertruck) can outnumber the server's own log files and
# so win the "most files" heuristic, yet are never the component wanted.
# timbertruck's JSON-lines are unparseable here regardless.
BLACKLISTED_BASES = frozenset(["timbertruck"])
LOW_PRIORITY_BASES = frozenset(["push-client"])


class LogFile:
    """A single rotated log file of a recognised YT log series."""

    def __init__(self, name, base, channel, rotation, directory):
        self.name = name
        self.base = base
        self.channel = channel
        self.rotation = rotation  # "" for the current file
        self.directory = directory  # remote dir holding this file

        self.is_current = (rotation == "")
        self.sequence = None
        self.timestamp = None
        if not self.is_current:
            m = TIMESTAMP_RE.match(rotation)
            if m:
                self.timestamp = datetime(*(int(g) for g in m.groups()))
            elif SEQUENCE_RE.match(rotation):
                self.sequence = int(rotation)

    @property
    def path(self):
        return "{}/{}".format(self.directory, self.name)


def parse_log_name(name, directory=REMOTE_LOGS_DIR):
    """Parses a directory entry into a LogFile, or returns None if it is not a
    recognised plain/zst/gz log file.

    Recognised shape: ``BASE[.channel].log[.rotation][.zst|.gz]`` where BASE is a
    single dot-free token (``node-vla5-2023``, ``timbertruck``, ...) and channel
    is ``debug``, ``error`` or empty (info). Anything with a multi-token channel
    (``lsm.json``, ``tablet_error.yson``) or a trailing index file (``.trindex``)
    is rejected.
    """
    stripped = name
    if stripped.endswith(".zst"):
        stripped = stripped[:-4]
    elif stripped.endswith(".gz"):
        stripped = stripped[:-3]

    tokens = stripped.split(".")
    try:
        log_index = tokens.index("log")
    except ValueError:
        return None

    base = tokens[0]
    channel = ".".join(tokens[1:log_index])
    rotation = ".".join(tokens[log_index + 1:])

    # Rotation must be empty (current), a sequence number, or a timestamp.
    if rotation and not (TIMESTAMP_RE.match(rotation) or SEQUENCE_RE.match(rotation)):
        return None

    return LogFile(name, base, channel, rotation, directory)


def order_series(parsed_files, log_type, component=None):
    """From already-parsed log files (all from the same logical source), returns
    the ordered (oldest -> newest) list for the requested type. Groups by base
    component and, if several components are present, picks the one with the most
    files. When ``component`` is supplied, only that exact base is eligible;
    this prevents a sidecar with more rotations from crossing the requested
    service boundary."""
    wanted_channel = CHANNEL_BY_TYPE[log_type]

    by_base = {}
    for parsed in parsed_files:
        if parsed is None or parsed.channel != wanted_channel:
            continue
        if parsed.base in BLACKLISTED_BASES:
            continue
        by_base.setdefault(parsed.base, []).append(parsed)

    if not by_base:
        return None, []

    if component is not None:
        if component not in by_base:
            return None, []
        base = component
    else:
        # Retained for library compatibility. The CLI always resolves an exact
        # component from the hostname or --component before reaching here.
        base = max(
            by_base,
            key=lambda b: (b not in LOW_PRIORITY_BASES, len(by_base[b]), b),
        )
    files = by_base[base]

    current = [f for f in files if f.is_current]
    timestamped = sorted((f for f in files if f.timestamp is not None),
                         key=lambda f: f.timestamp)
    sequenced = sorted((f for f in files if f.sequence is not None),
                       key=lambda f: f.sequence, reverse=True)

    # Oldest first; the current (un-rotated) file is the newest.
    ordered = sequenced + timestamped + current
    return base, ordered


# Archive subdirectories are named by calendar day: "YYYY-MM-DD".
ARCHIVE_DAY_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")


def list_remote_dir(ssh, directory):
    """Lists a remote directory, returning the entry names. Missing/unreadable
    directories yield an empty list instead of aborting."""
    out = ssh.run(["ls", "-1", directory], check=False)
    return [line for line in out.splitlines() if line]


def discover_live(ssh, log_type, component=None):
    """The live ``logs`` directory: returns (base, ordered_files)."""
    parsed = [parse_log_name(name, REMOTE_LOGS_DIR)
              for name in list_remote_dir(ssh, REMOTE_LOGS_DIR)]
    return order_series(parsed, log_type, component)


def archive_day_dirs(names, start_time, end_time):
    """From the archive root listing, the day subdirectories overlapping the
    window. A one-day margin on each side covers files whose content spills past
    the day boundary named in their path."""
    from datetime import timedelta
    lo = (start_time.date() - timedelta(days=1)) if start_time else None
    hi = (end_time.date() + timedelta(days=1)) if end_time else None
    days = []
    for name in names:
        m = ARCHIVE_DAY_RE.match(name)
        if not m:
            continue
        day = datetime(*(int(g) for g in m.groups())).date()
        if (lo is None or day >= lo) and (hi is None or day <= hi):
            days.append(name)
    return sorted(days)


def discover_archive(ssh, log_type, start_time, end_time, archive_dir,
                     component=None):
    """The master log archive: returns (base, ordered_files). Only the day
    subdirectories overlapping the window are scanned. Consulted only when a
    window bound is given (an unbounded scan of the whole archive is never what
    is wanted); returns (None, []) when the archive is absent or out of range."""
    if archive_dir is None or (start_time is None and end_time is None):
        return None, []
    days = archive_day_dirs(list_remote_dir(ssh, archive_dir), start_time, end_time)
    parsed = []
    for day in days:
        directory = "{}/{}".format(archive_dir, day)
        for name in list_remote_dir(ssh, directory):
            parsed.append(parse_log_name(name, directory))
    return order_series(parsed, log_type, component)


########################################################################
# Time handling (good enough for file selection; logslice does the real work).
########################################################################

MONTH_BY_NAME = {name.lower(): i + 1 for i, name in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])}


def parse_user_time(text):
    """Parses a -t/-e value into a naive local datetime for file selection.

    Mirrors the common formats understood by logslice. Returns None if the
    format is not recognised (the caller then falls back to a full search)."""
    text = text.strip()
    if not text:
        return None
    if text.lower() == "now":
        return datetime.now()

    # ISO UTC "2019-09-19T11:46:04.848360Z" -> compare against local times.
    iso = re.match(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})"
                   r"(?:\.(\d+))?Z$", text)
    if iso:
        from datetime import timezone
        y, mo, d, h, mi, s = (int(iso.group(i)) for i in range(1, 7))
        micro = int((iso.group(7) or "0").ljust(6, "0")[:6])
        utc = datetime(y, mo, d, h, mi, s, micro, tzinfo=timezone.utc)
        return utc.astimezone().replace(tzinfo=None)

    # Local timestamp at a flexible level of detail: a bare date "YYYY-MM-DD",
    # or with a progressively finer time-of-day suffix " HH", " HH:MM",
    # " HH:MM:SS" and an optional "[,.]uuuuuu" subsecond (the fraction needs
    # seconds). Absent fields default to zero, so a bare date is that day's
    # midnight. Mirrors TryParseLocalFull in logslice/lib/time_parser.h.
    full = re.match(r"^(\d{4})-(\d{2})-(\d{2})"
                    r"(?:[ T](\d{2})(?::(\d{2})(?::(\d{2})(?:[.,](\d+))?)?)?)?$",
                    text)
    if full:
        y, mo, d = (int(full.group(i)) for i in range(1, 4))
        h = int(full.group(4) or 0)
        mi = int(full.group(5) or 0)
        s = int(full.group(6) or 0)
        micro = int((full.group(7) or "0").ljust(6, "0")[:6])
        # A well-formed but out-of-range date (e.g. month 13) is treated as
        # unrecognised so the caller falls back to a full scan rather than
        # crashing; datetime is stricter here than the C++ mktime path.
        try:
            return datetime(y, mo, d, h, mi, s, micro)
        except ValueError:
            return None

    # "HH:MM" or "HH:MM:SS" -> today's date.
    tod = re.match(r"^(\d{2}):(\d{2})(?::(\d{2}))?$", text)
    if tod:
        now = datetime.now()
        return now.replace(hour=int(tod.group(1)), minute=int(tod.group(2)),
                           second=int(tod.group(3) or 0), microsecond=0)

    # Web UI format "16 Nov 2018 13:56:14" (local time): 1-2 digit day, a
    # case-insensitive 3-letter month name, 4-digit year and HH:MM:SS.
    web = re.match(r"^(\d{1,2}) ([A-Za-z]{3}) (\d{4}) "
                   r"(\d{2}):(\d{2}):(\d{2})$", text)
    if web:
        month = MONTH_BY_NAME.get(web.group(2).lower())
        if month is not None:
            return datetime(int(web.group(3)), month, int(web.group(1)),
                            int(web.group(4)), int(web.group(5)),
                            int(web.group(6)))

    return None


def split_time_range(text):
    """Splits a combined "start - end" -t value into (start, end); returns
    (text, None) when the " - " separator is absent. The separator is a
    space-padded hyphen, so the bare hyphens inside dates and times are left
    untouched."""
    parts = text.split(" - ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return text, None


INFO_RE = re.compile(
    r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}),(\d{6})")


def parse_info_time(text):
    m = INFO_RE.search(text)
    if not m:
        return None
    return datetime(*(int(m.group(i)) for i in range(1, 7)),
                    microsecond=int(m.group(7)))


########################################################################
# File selection driven by logslice --info.
########################################################################

class FileSelector:
    def __init__(self, ssh, remote_bin, files):
        self.ssh = ssh
        self.remote_bin = remote_bin
        self.files = files
        self._info_cache = {}

    def info(self, index):
        """Returns (first_dt, last_dt) for files[index], or (None, None).

        `logslice --info` intermittently fails on .zst files with a transient
        broken-pipe / Bad-file-descriptor error (non-deterministic; a retry
        usually succeeds). This retries a few times; if it still fails it
        returns (None, None) and warns, so the selector falls back to the
        rotation-timestamp hint for that file instead of aborting the whole
        search — one flaky file must not kill the run.
        """
        if index not in self._info_cache:
            path = self.files[index].path
            out = self.ssh.run(
                [self.remote_bin, "--info", path],
                check=False,
                warn_on_error=True,
                retries=2,
                retry_on_empty=True)
            first = last = None
            if not out:
                # Probe failed after retries: don't abort — fall back to the
                # hint-based bounds. Warn loudly so the empty result is not
                # mistaken for "file has no timestamps".
                eprint("Warning: `logslice --info {}` failed after retries; "
                       "falling back to the rotation-timestamp hint for this "
                       "file (it may be scanned even if it doesn't overlap the "
                       "window).".format(path))
            else:
                for line in out.splitlines():
                    if line.startswith("first:"):
                        first = parse_info_time(line)
                    elif line.startswith("last:"):
                        last = parse_info_time(line)
                if first is None and last is None:
                    eprint("Warning: `logslice --info {}` returned no "
                           "timestamps (empty output); falling back to the "
                           "rotation-timestamp hint.".format(path))
            self._info_cache[index] = (first, last)
        return self._info_cache[index]

    # Rotation timestamps are only an approximation of a file's content range: a
    # file rotated at HH:MM usually still holds a few records stamped just after
    # HH:MM. So hints are used only to bound the search range (with a safety
    # margin); the exact overlap is always decided by --info. Without this margin
    # a boundary file could be pruned away before the search ever looks at it.
    HINT_MARGIN = 1

    def hint_bounds(self, start_time, end_time):
        """Uses the rotation timestamps (when present) to narrow the index range
        that the --info binary search must scan. Returns an inclusive, deliberately
        generous [lo, hi] that is guaranteed to contain every overlapping file.
        For sequence-numbered series (no time hints) this is the full range."""
        n = len(self.files)
        lo, hi = 0, n - 1
        ends = [f.timestamp for f in self.files]  # None for current/sequence

        if start_time is not None:
            # Largest leading file that nominally ends before start_time; keep it
            # (its tail may spill into the window) and drop everything older.
            p = -1
            for i in range(n):
                if ends[i] is not None and ends[i] < start_time:
                    p = i
                else:
                    break
            if p >= 0:
                lo = max(0, p - self.HINT_MARGIN)
        if end_time is not None:
            # First file that nominally starts after end_time (its predecessor's
            # rotation time is already past the window); keep everything before it.
            q = n
            for i in range(n):
                prev_end = ends[i - 1] if i - 1 >= 0 else None
                if prev_end is not None and prev_end > end_time:
                    q = i
                    break
            hi = min(n - 1, (q - 1) + self.HINT_MARGIN)
        if lo > hi:
            lo, hi = min(lo, hi), max(lo, hi)
        return lo, hi

    def first_overlapping(self, start_time, lo, hi):
        """Smallest index in [lo, hi] whose last record is >= start_time, or None.
        A file with no timestamped lines is treated as a candidate."""
        a, b = lo, hi
        step = 1
        a = hi - step
        while a > lo:
            first, _ = self.info(a)
            if first is None or first < start_time:
                break
            step *= 2
            a = hi - step
        if a < lo:
            a = lo
        result = None
        while a <= b:
            mid = (a + b) // 2
            _, last = self.info(mid)
            if last is None or last >= start_time:
                result = mid
                b = mid - 1
            else:
                a = mid + 1
        return result

    def last_overlapping(self, end_time, lo, hi):
        """Largest index in [lo, hi] whose first record is <= end_time, or None."""
        a, b = lo, hi
        step = 1
        a = hi - step
        while a > lo:
            _, last = self.info(a)
            if last is None or last < end_time:
                break
            step *= 2
            a = hi - step
        if a < lo:
            a = lo
        result = None
        while a <= b:
            mid = (a + b) // 2
            first, _ = self.info(mid)
            if first is None or first <= end_time:
                result = mid
                a = mid + 1
            else:
                b = mid - 1
        return result

    def select(self, start_time, end_time):
        n = len(self.files)
        if n == 0:
            return []
        lo, hi = self.hint_bounds(start_time, end_time)

        start_index = lo if start_time is None \
            else self.first_overlapping(start_time, lo, hi)
        end_index = hi if end_time is None \
            else self.last_overlapping(end_time, lo, hi)

        if start_index is None or end_index is None or start_index > end_index:
            return []
        return self.files[start_index:end_index + 1]


def _candidate_bases(names, log_type, directory=REMOTE_LOGS_DIR):
    wanted_channel = CHANNEL_BY_TYPE[log_type]
    return {
        parsed.base
        for parsed in (parse_log_name(name, directory) for name in names)
        if parsed is not None and parsed.channel == wanted_channel
        and parsed.base not in BLACKLISTED_BASES
    }


def discover_component_candidates(ssh, log_type, start_time, end_time,
                                  archive_dir):
    """Return discovered component bases and every directory inspected.

    Only directory entries are read here. Log contents are untouched until the
    caller resolves an exact component and starts ``FileSelector``.
    """
    roots = [REMOTE_LOGS_DIR]
    candidates = _candidate_bases(
        list_remote_dir(ssh, REMOTE_LOGS_DIR), log_type
    )
    if archive_dir is not None and (start_time is not None or end_time is not None):
        days = archive_day_dirs(
            list_remote_dir(ssh, archive_dir), start_time, end_time
        )
        roots.append(archive_dir)
        for day in days:
            directory = "{}/{}".format(archive_dir, day)
            roots.append(directory)
            candidates.update(_candidate_bases(
                list_remote_dir(ssh, directory), log_type, directory
            ))
    return sorted(candidates), roots


def infer_host_component(host):
    """Infer ``(role, component)`` from known YP pod hostname markers."""
    short = host.split(".", 1)[0].lower()
    node = re.search(
        r"(?:^|-)(?P<role>tab|dat|exec)-(?:node|sen)(?:-|$)",
        short,
    )
    if node:
        role = {
            "tab": "tablet-node",
            "dat": "data-node",
            "exec": "exec-node",
        }[node.group("role")]
        return role, "node"
    if re.search(r"(?:^|-)master-cache(?:-|$)", short):
        return "master-cache", "master-cache"
    if re.search(r"(?:^|-)rpc(?:-proxy)?(?:-|$)", short):
        location = re.match(r"^(?P<location>[a-z]{3}\d+-\d+)(?:-|$)", short)
        return "rpc-proxy", (
            "proxy-" + location.group("location") if location else "proxy"
        )
    if re.search(r"(?:^|-)http-proxy(?:-|$)", short) or \
            re.search(r"(?:^|-)proxy(?:-|$)", short):
        location = re.match(r"^(?P<location>[a-z]{3}\d+-\d+)(?:-|$)", short)
        return "http-proxy", (
            "proxy-" + location.group("location") if location else "proxy"
        )
    if re.search(r"(?:^|-)clock\d*(?:-|$)", short):
        return "clock", "clock"
    if re.search(r"(?:^|-)master(?:-|$)", short) or \
            re.match(r"^m(?:c)?\d+(?:-|$)", short):
        return "master", "master"
    return None, None


def _resolve_base(role, component, available):
    if component in available:
        return component
    if role == "master":
        matches = [
            candidate for candidate in available
            if candidate.startswith("master-")
            and not candidate.startswith("master-cache")
        ]
    else:
        prefix = component + "-"
        matches = [
            candidate for candidate in available
            if candidate.startswith(prefix)
        ]
    if not matches:
        return None
    return sorted(
        matches,
        key=lambda candidate: (candidate in LOW_PRIORITY_BASES, candidate),
    )[0]


def resolve_component_route(host, override, candidates):
    """Resolve one exact component or raise with the discovered candidates."""
    available = sorted(set(candidates))
    shown = ", ".join(available) if available else "(none)"
    if override:
        if override not in available:
            raise ValueError(
                "--component {!r} was not found; discovered: {}".format(
                    override, shown
                )
            )
        return {
            "role": "explicit",
            "component": override,
            "base": override,
            "source": "--component",
            "confidence": "override",
        }

    role, component = infer_host_component(host)
    if component is None:
        raise ValueError(
            "cannot infer the YT component from host {!r}; discovered: {}; "
            "pass --component NAME".format(host, shown)
        )
    base = _resolve_base(role, component, available)
    if base is None:
        raise ValueError(
            "host {!r} maps to role={} component={!r}, but that base was not "
            "found; discovered: {}; pass --component NAME only after verifying "
            "the service boundary".format(host, role, component, shown)
        )
    return {
        "role": role,
        "component": component,
        "base": base,
        "source": "hostname",
        "confidence": "high",
    }


def routing_metadata(route, roots):
    base_suffix = ""
    if route["base"] != route["component"]:
        base_suffix = " base={}".format(route["base"])
    return [
        "Log routing: role={role} component={component}{base_suffix} "
        "source={source} confidence={confidence}".format(
            base_suffix=base_suffix, **route),
        "Resolved log roots: " + ", ".join(roots),
    ]


def should_use_master_archive(host, override):
    if override:
        return (
            override == "master"
            or (override.startswith("master-")
                and not override.startswith("master-cache"))
        )
    role, _ = infer_host_component(host)
    return role == "master"


def discover_series(ssh, log_type, start_time, end_time, archive_dir,
                    component=None):
    """The log series to search, ordered oldest -> newest: the archive (when a
    window is given and day subdirectories are in range) followed by the live
    ``logs`` directory. Each entry is an ``(origin, base, ordered_files)`` tuple;
    a series with no files is dropped."""
    series = []
    archive_base, archive_files = discover_archive(
        ssh, log_type, start_time, end_time, archive_dir, component)
    if archive_files:
        series.append(("archive", archive_base, archive_files))
    live_base, live_files = discover_live(ssh, log_type, component)
    if live_files:
        series.append(("live", live_base, live_files))
    return series


def select_log_files(ssh, remote_bin, series, start_time, end_time):
    """Selects the overlapping files from every series independently and returns
    the flat oldest -> newest list, plus a per-series ``(origin, base, total,
    selected)`` summary for logging.

    Selecting each series on its own is what makes a window that straddles the
    archive/live boundary work: each binary search runs over a single monotonic
    ordering, so the tail of the archive and the head of the live logs are both
    picked up. Merging the two into one list would break that monotonicity,
    because live files may be sequence-numbered while archive files are
    timestamped."""
    selected = []
    summary = []
    for origin, base, files in series:
        sel = FileSelector(ssh, remote_bin, files).select(start_time, end_time)
        summary.append((origin, base, len(files), sel))
        selected.extend(sel)
    return selected, summary


########################################################################
# Post-processing pipeline (whitelisted unix tools run after logslice).
########################################################################

def split_pipeline(text):
    """Splits a shell-like pipeline string ("grep Error | wc -l") into a list of
    stages, each a list of argv tokens. A '|' is a stage separator even when glued
    to an adjacent word ("foo |grep", "foo| grep", "foo|grep"); a '|' inside quotes
    stays a literal argument. The punctuation_chars lexer gives us both: it emits
    '|' as its own token regardless of surrounding whitespace, while honouring
    quoting. The whitelist is enforced later by Ssh.validate_pipeline."""
    lexer = shlex.shlex(text, posix=True, punctuation_chars="|")
    lexer.whitespace_split = True
    stages, current = [], []
    for tok in lexer:
        if tok == "|":
            stages.append(current)
            current = []
        else:
            current.append(tok)
    stages.append(current)
    return stages


########################################################################
# Main.
########################################################################

def split_argv(argv):
    """Splits argv at the first '--': left side is parsed as options, right side
    is the verbatim grep argument list."""
    if "--" in argv:
        idx = argv.index("--")
        return argv[:idx], argv[idx + 1:]
    return argv, []


def main():
    left, grep_args = split_argv(sys.argv[1:])

    parser = argparse.ArgumentParser(
        prog="logslice.py",
        description="Remote log grepping via logslice.",
        usage="%(prog)s remote_machine_name [--type type] [-l logslice] "
              "[-t start_time] [-e end_time] [-x pipeline] -- grep_args...")
    parser.add_argument("host", help="remote machine name")
    parser.add_argument("--type", default="debug",
                        help="log type: debug, error, info, comma-separated "
                             "types, or all (default: debug)")
    parser.add_argument(
        "--component",
        default=None,
        help="exact log base to select; overrides hostname-derived routing",
    )
    parser.add_argument("-l", dest="logslice", default=None,
                        help="path to a logslice binary")
    parser.add_argument("-t", dest="start", default=None,
                        help="time window start (passed to logslice); may also "
                             "carry the whole window as \"start - end\", which "
                             "fills -e when -e is not given")
    parser.add_argument("-e", dest="end", default=None,
                        help="time window end (passed to logslice)")
    parser.add_argument("--archive-dir", dest="archive_dir",
                        default=ARCHIVE_DIR_DEFAULT,
                        help="root of the per-day master log archive to also "
                             "search (default: {}); pass '' to disable."
                             .format(ARCHIVE_DIR_DEFAULT))
    parser.add_argument("-x", dest="execute", default=None,
                        help="pipe-separated post-processing commands run after "
                             "logslice, e.g. \"grep Error | wc -l\". "
                             "Only whitelisted tools are allowed: "
                             + ", ".join(sorted(Ssh.PIPELINE_WHITELIST)))
    parser.add_argument("--control-socket", dest="control_socket", default=None,
                        help="path to an existing ssh ControlMaster socket for "
                             "this host to reuse (no new connection or key touch "
                             "when the socket is live). Open one with: ssh -M "
                             "-S <path> -o ControlPersist={} "
                             "<host> true, then pass <path> here across calls."
                             .format(CONTROL_PERSIST_DEFAULT))
    parser.add_argument(
        "--control-persist",
        type=int,
        default=None,
        help="seconds to keep the ssh master alive after use (default: {})"
             .format(CONTROL_PERSIST_DEFAULT))
    parser.add_argument(
        "--connect-timeout",
        type=int,
        default=CONNECT_TIMEOUT_DEFAULT,
        help="ssh connection timeout in seconds (default: {})"
             .format(CONNECT_TIMEOUT_DEFAULT))
    parser.add_argument(
        "--allow-broad-debug", action="store_true",
        help="explicitly allow an unbounded or >60-second debug scan; first "
             "locate exact transitions in error logs or Monium")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="print ssh commands to stderr")
    args = parser.parse_args(left)

    # Build and validate the post-processing pipeline before any slow work (binary
    # resolution/build, ssh, scp), so a wrong argument is reported instantly.
    # Legacy "-- grep_args" is just a leading grep stage; -x appends arbitrary
    # whitelisted stages after it. All filtering is done by real remote tools, not
    # by logslice's own -g option.
    try:
        log_types = parse_log_types(args.type)
    except ValueError as ex:
        parser.error(str(ex))

    stages = []
    if grep_args:
        stages.append(["grep"] + grep_args)
    if args.execute:
        stages += split_pipeline(args.execute)
    try:
        Ssh.validate_pipeline(stages)
    except ValueError as ex:
        sys.exit(str(ex))

    # A single -t may carry the whole window as "start - end"; its right-hand
    # side fills -e unless -e was given explicitly.
    if args.start:
        args.start, end_from_start = split_time_range(args.start)
        if end_from_start is not None and not args.end:
            args.end = end_from_start

    start_time = parse_user_time(args.start) if args.start else None
    end_time = parse_user_time(args.end) if args.end else None
    if args.start and start_time is None:
        eprint("Warning: could not parse start time {!r}; "
               "scanning all files.".format(args.start))
    if args.end and end_time is None:
        eprint("Warning: could not parse end time {!r}; "
               "scanning all files.".format(args.end))
    try:
        validate_debug_window(
            log_types, start_time, end_time, args.allow_broad_debug)
    except ValueError as ex:
        parser.error(str(ex))

    ssh = Ssh(
        args.host,
        args.verbose,
        control_socket=args.control_socket,
        control_persist=args.control_persist,
        connect_timeout=args.connect_timeout)
    try:
        ssh.connect()
    except SystemExit as error:
        if preflight_failure_class(error) == "authentication_unavailable":
            report_authentication_preflight(
                args.host,
                requested_component(args.host, args.component),
                args.start,
                args.end)
            return OPERATIONAL_FAILURE_EXIT
        raise

    local_bin = resolve_logslice(args.logslice)
    ssh.copy_binary(local_bin, REMOTE_BIN)
    server_timezone = ssh.run(
        ["date", "+%z"], check=False, warn_on_error=True).strip() or "unknown"

    # The archive (older, time-named files) is searched alongside the live logs;
    # discover_series returns them oldest -> newest and select_log_files selects
    # each independently, so a window straddling the archive/live boundary picks
    # up files from both.
    archive_dir = args.archive_dir or None
    if archive_dir is not None and not should_use_master_archive(
            args.host, args.component):
        archive_dir = None
    candidates = set()
    roots = []
    for log_type in log_types:
        type_candidates, type_roots = discover_component_candidates(
            ssh, log_type, start_time, end_time, archive_dir)
        candidates.update(type_candidates)
        for root in type_roots:
            if root not in roots:
                roots.append(root)
    try:
        route = resolve_component_route(args.host, args.component, candidates)
    except ValueError as error:
        sys.exit(str(error))
    for line in routing_metadata(route, roots):
        eprint(line)

    selected = []
    selected_paths = []
    for log_type in log_types:
        series = discover_series(
            ssh, log_type, start_time, end_time, archive_dir,
            component=route["base"])
        if not series:
            eprint("Found 0 {} log files on {}.".format(log_type, args.host))
            continue
        type_selected, summary = select_log_files(
            ssh, REMOTE_BIN, series, start_time, end_time)
        for origin, base, total, sel in summary:
            eprint("Found {} {} log file(s) for component '{}' ({})."
                   .format(total, log_type, base, origin))
            if sel:
                eprint("Selected {} {} file(s): {} .. {}".format(
                    len(sel), origin, sel[0].name, sel[-1].name))
        for log_file in type_selected:
            selected.append((log_type, log_file))
            selected_paths.append(log_file.path)

    results = []
    outputs = []
    for log_type, log_file in selected:
        path = log_file.path
        head = [REMOTE_BIN]
        # Pass the window bounds to every selected file: the boundary files need
        # them and for the interior files they are a harmless no-op. This keeps
        # the output correct even when the conservative selection includes a file
        # that is not fully inside the window.
        if args.start:
            head += ["-t", args.start]
        if args.end:
            head += ["-e", args.end]
        head.append(path)
        completed = ssh.run_pipeline_result(head, stages, capture=True)
        status = classify_slice_result(
            completed.returncode, completed.stdout, completed.stderr)
        result = {
            "type": log_type,
            "file": path,
            "status": status,
            "returncode": completed.returncode,
            "match_count": len(completed.stdout.splitlines()),
        }
        if status == "failed":
            result["failure_class"] = slice_failure_class(
                completed.returncode, completed.stderr)
            result["error"] = completed.stderr.strip() or describe_exit_code(
                completed.returncode)
        results.append(result)
        if status == "matched":
            outputs.append(completed.stdout)
        detail = ""
        if status == "failed":
            detail = " failure_class={}".format(result["failure_class"])
        eprint("file_status type={} status={} matches={} file={}{}".format(
            log_type, status, result["match_count"], path, detail))

    if outputs:
        sys.stdout.write(merge_timestamped_outputs(outputs))

    exit_code = slice_exit_code(results)
    matched = sum(item["match_count"] for item in results
                  if item["status"] == "matched")
    failure_class = "none"
    failed_classes = sorted({
        item["failure_class"] for item in results
        if item["status"] == "failed"
    })
    if failed_classes:
        failure_class = ",".join(failed_classes)
    elif exit_code == GLOBAL_NO_MATCH_EXIT:
        failure_class = "global_no_match"
    eprint(
        "summary timezone={} window_start={} window_end={} files={} "
        "matches={} failure_class={} exit_code={}".format(
            server_timezone, args.start or "open", args.end or "open",
            len(selected_paths), matched, failure_class, exit_code))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
