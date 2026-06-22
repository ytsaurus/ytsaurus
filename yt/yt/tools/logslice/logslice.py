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
import os
import re
import shlex
import subprocess
import sys
import tempfile
from datetime import datetime


REMOTE_DIR = "/tmp"
REMOTE_BIN = REMOTE_DIR + "/logslice"
REMOTE_LOGS_DIR = "logs"

# Old rotated master logs are moved here, under per-day subdirectories named
# "YYYY-MM-DD", e.g. /yt/master-logs-archive/2026-06-19/master-klg0-0941.debug.log.2026-06-19_07-15.zst
ARCHIVE_DIR_DEFAULT = "/yt/master-logs-archive"

# A debug logslice is hundreds of MiB; a release one is well under this. Anything
# below the limit is assumed to be a usable release build.
RELEASE_SIZE_LIMIT = 50 * 1024 * 1024

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
# SSH / SCP helpers (connection multiplexed so we authenticate once).
########################################################################

class Ssh:
    # Only these programs may start a remote pipeline stage. Validation lives here,
    # right next to the ssh invocation, so there is no path that reaches the remote
    # shell without passing the whitelist; stage arguments are always shlex-quoted.
    # Do NOT add "awk" in this list (because of `system()` call).
    PIPELINE_WHITELIST = frozenset(["grep", "wc", "cut", "sed", "head", "tail"])

    def __init__(self, host, verbose=False):
        self.host = host
        self.verbose = verbose
        self._control_path = os.path.join(
            tempfile.gettempdir(), "logslice_ssh_%r@%h:%p")
        self._base_opts = [
            "-o", "ControlMaster=auto",
            "-o", "ControlPath=" + self._control_path,
            "-o", "ControlPersist=120",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=30",
        ]

    def connect(self):
        """Establishes the master connection (this is where the key touch
        happens, exactly once)."""
        eprint("Connecting to {} (you may need to touch your security key)..."
               .format(self.host))
        self.run(["true"])

    def _remote_command(self, argv):
        return " ".join(shlex.quote(token) for token in argv)

    def run(self, argv, capture=True, check=True):
        """Runs argv on the remote host. Returns stdout (text) when capture is
        set, otherwise streams stdout/stderr straight through.

        With check=False a non-zero exit is not fatal: the (possibly empty)
        stdout is returned instead of aborting. Used for optional probes such as
        listing the log archive, which simply does not exist on most hosts."""
        cmd = ["ssh"] + self._base_opts + [self.host, self._remote_command(argv)]
        if self.verbose:
            eprint("Executing: {}".format(" ".join(shlex.quote(c) for c in cmd)))
        if capture:
            result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True)
            if result.returncode != 0 and check:
                sys.exit("ssh {} failed: {}".format(
                    " ".join(argv), result.stderr.strip()))
            return result.stdout
        return subprocess.run(cmd).returncode

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
        nothing can break out into shell syntax. Returns stdout when capture is set,
        otherwise the exit code."""
        self.validate_pipeline(stages)
        remote_command = self._remote_command(head_argv)
        for stage in stages:
            remote_command += " | " + self._remote_command(stage)
        cmd = ["ssh"] + self._base_opts + [self.host, remote_command]
        if self.verbose:
            eprint("Executing: {}".format(" ".join(shlex.quote(c) for c in cmd)))
        if capture:
            result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                sys.exit("ssh command failed: {}".format(result.stderr.strip()))
            return result.stdout
        return subprocess.run(cmd).returncode

    def scp(self, local_path, remote_path):
        cmd = (["scp"] + self._base_opts +
               [local_path, "{}:{}".format(self.host, remote_path)])
        if self.verbose:
            eprint("Executing: {}".format(" ".join(shlex.quote(c) for c in cmd)))
        result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            sys.exit("scp failed: {}".format(result.stderr.strip()))


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


def order_series(parsed_files, log_type):
    """From already-parsed log files (all from the same logical source), returns
    the ordered (oldest -> newest) list for the requested type. Groups by base
    component and, if several components are present, picks the one with the most
    files."""
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

    # Prefer the component with the most log files (the primary YT server).
    base = max(by_base, key=lambda b: len(by_base[b]))
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


def discover_live(ssh, log_type):
    """The live ``logs`` directory: returns (base, ordered_files)."""
    parsed = [parse_log_name(name, REMOTE_LOGS_DIR)
              for name in list_remote_dir(ssh, REMOTE_LOGS_DIR)]
    return order_series(parsed, log_type)


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


def discover_archive(ssh, log_type, start_time, end_time, archive_dir):
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
    return order_series(parsed, log_type)


########################################################################
# Time handling (good enough for file selection; logslice does the real work).
########################################################################

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

    # "YYYY-MM-DD HH:MM:SS[,uuuuuu | .uuuuuu]" and "YYYY-MM-DD HH:MM".
    full = re.match(r"^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})"
                    r"(?::(\d{2}))?(?:[.,](\d+))?$", text)
    if full:
        y, mo, d, h, mi = (int(full.group(i)) for i in range(1, 6))
        s = int(full.group(6) or 0)
        micro = int((full.group(7) or "0").ljust(6, "0")[:6])
        return datetime(y, mo, d, h, mi, s, micro)

    # "HH:MM" or "HH:MM:SS" -> today's date.
    tod = re.match(r"^(\d{2}):(\d{2})(?::(\d{2}))?$", text)
    if tod:
        now = datetime.now()
        return now.replace(hour=int(tod.group(1)), minute=int(tod.group(2)),
                           second=int(tod.group(3) or 0), microsecond=0)

    return None


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
        """Returns (first_dt, last_dt) for files[index], or (None, None)."""
        if index not in self._info_cache:
            path = self.files[index].path
            out = self.ssh.run([self.remote_bin, "--info", path])
            first = last = None
            for line in out.splitlines():
                if line.startswith("first:"):
                    first = parse_info_time(line)
                elif line.startswith("last:"):
                    last = parse_info_time(line)
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


def discover_series(ssh, log_type, start_time, end_time, archive_dir):
    """The log series to search, ordered oldest -> newest: the archive (when a
    window is given and day subdirectories are in range) followed by the live
    ``logs`` directory. Each entry is an ``(origin, base, ordered_files)`` tuple;
    a series with no files is dropped."""
    series = []
    archive_base, archive_files = discover_archive(
        ssh, log_type, start_time, end_time, archive_dir)
    if archive_files:
        series.append(("archive", archive_base, archive_files))
    live_base, live_files = discover_live(ssh, log_type)
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
                        choices=["debug", "error", "info"],
                        help="log type: debug, error or info (default: debug)")
    parser.add_argument("-l", dest="logslice", default=None,
                        help="path to a logslice binary")
    parser.add_argument("-t", dest="start", default=None,
                        help="time window start (passed to logslice)")
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
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="print ssh commands to stderr")
    args = parser.parse_args(left)

    # Build and validate the post-processing pipeline before any slow work (binary
    # resolution/build, ssh, scp), so a wrong argument is reported instantly.
    # Legacy "-- grep_args" is just a leading grep stage; -x appends arbitrary
    # whitelisted stages after it. All filtering is done by real remote tools, not
    # by logslice's own -g option.
    stages = []
    if grep_args:
        stages.append(["grep"] + grep_args)
    if args.execute:
        stages += split_pipeline(args.execute)
    try:
        Ssh.validate_pipeline(stages)
    except ValueError as ex:
        sys.exit(str(ex))

    local_bin = resolve_logslice(args.logslice)

    start_time = parse_user_time(args.start) if args.start else None
    end_time = parse_user_time(args.end) if args.end else None
    if args.start and start_time is None:
        eprint("Warning: could not parse start time {!r}; "
               "scanning all files.".format(args.start))
    if args.end and end_time is None:
        eprint("Warning: could not parse end time {!r}; "
               "scanning all files.".format(args.end))

    ssh = Ssh(args.host, args.verbose)
    ssh.connect()
    ssh.scp(local_bin, REMOTE_BIN)
    ssh.run(["chmod", "+x", REMOTE_BIN])

    # The archive (older, time-named files) is searched alongside the live logs;
    # discover_series returns them oldest -> newest and select_log_files selects
    # each independently, so a window straddling the archive/live boundary picks
    # up files from both.
    series = discover_series(
        ssh, args.type, start_time, end_time, args.archive_dir or None)
    if not series:
        sys.exit("No {} log files found on {}.".format(args.type, args.host))

    selected, summary = select_log_files(
        ssh, REMOTE_BIN, series, start_time, end_time)
    for origin, base, total, sel in summary:
        eprint("Found {} {} log file(s) for component '{}' ({})."
               .format(total, args.type, base, origin))
        if sel:
            eprint("Selected {} {} file(s): {} .. {}".format(
                len(sel), origin, sel[0].name, sel[-1].name))

    if not selected:
        eprint("No log files overlap the requested time window.")
        return

    failures = []
    for log_file in selected:
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
        returncode = ssh.run_pipeline(head, stages, capture=False)
        if returncode != 0:
            failures.append((log_file.name, returncode))
            eprint("logslice failed on {} ({}).".format(
                log_file.name, describe_exit_code(returncode)))

    if failures:
        sys.exit("logslice failed on {} of {} file(s).".format(
            len(failures), len(selected)))


if __name__ == "__main__":
    main()
