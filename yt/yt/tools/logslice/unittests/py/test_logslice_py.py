"""Unit tests for logslice.py.

The pipeline-parsing/quoting tests are pure and always run. The integration
tests actually shell out over `ssh localhost`; they are skipped automatically
when password-less ssh to localhost is unavailable.
"""

import contextlib
import importlib.util
import os
import shlex
import subprocess
import unittest


ARCADIA_PATH = "yt/yt/tools/logslice/logslice.py"


def _find_logslice_py():
    # Under `ya make -t` the sources live in the arcadia source tree, located via
    # yatest. For a standalone `python3 -m unittest` run, walk up from this file.
    try:
        import yatest.common
        return yatest.common.source_path(ARCADIA_PATH)
    except ImportError:
        pass
    directory = os.path.dirname(os.path.realpath(__file__))
    while True:
        candidate = os.path.join(directory, "logslice.py")
        if os.path.isfile(candidate):
            return candidate
        parent = os.path.dirname(directory)
        if parent == directory:
            raise RuntimeError("logslice.py not found")
        directory = parent


def _load_logslice():
    path = _find_logslice_py()
    spec = importlib.util.spec_from_file_location("logslice", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


logslice = _load_logslice()


def _ssh_localhost_works():
    try:
        return subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=5", "localhost", "true"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    except OSError:
        return False


@contextlib.contextmanager
def _silence_fd_output():
    """Silences OS-level stdout/stderr (fds 1 and 2) for the duration of the
    block. The injection-probe pipelines run remote grep with capture=False, so
    grep's harmless 'No such file or directory' chatter (the injected words are
    literal grep arguments) would otherwise leak into the test runner's output.
    Operates on file descriptors, not sys.stderr, because the noise originates in
    a subprocess."""
    with open(os.devnull, "w") as devnull:
        saved_out, saved_err = os.dup(1), os.dup(2)
        try:
            os.dup2(devnull.fileno(), 1)
            os.dup2(devnull.fileno(), 2)
            yield
        finally:
            os.dup2(saved_out, 1)
            os.dup2(saved_err, 2)
            os.close(saved_out)
            os.close(saved_err)


class SplitPipelineTest(unittest.TestCase):
    def test_splits_stages_on_pipe(self):
        self.assertEqual(
            logslice.split_pipeline("grep Error | wc -l"),
            [["grep", "Error"], ["wc", "-l"]])

    def test_single_stage(self):
        self.assertEqual(logslice.split_pipeline("wc -l"), [["wc", "-l"]])

    def test_pipe_glued_to_following_word(self):
        # "|grep" must still split into a separator and a new stage.
        self.assertEqual(
            logslice.split_pipeline('grep "Request acknowledged" |grep -v Bus'),
            [["grep", "Request acknowledged"], ["grep", "-v", "Bus"]])

    def test_pipe_glued_to_preceding_word(self):
        self.assertEqual(
            logslice.split_pipeline("grep Error| wc -l"),
            [["grep", "Error"], ["wc", "-l"]])

    def test_pipe_glued_to_both_words(self):
        self.assertEqual(
            logslice.split_pipeline("grep Error|wc -l"),
            [["grep", "Error"], ["wc", "-l"]])

    def test_quoted_pipe_is_not_a_separator(self):
        # The '|' lives inside a quoted grep pattern, so it stays one argument.
        self.assertEqual(
            logslice.split_pipeline("grep -E 'foo|bar'"),
            [["grep", "-E", "foo|bar"]])

    def test_semicolon_is_not_a_separator(self):
        # Only '|' separates stages. A ';' glued to a word is not punctuation, so
        # it stays part of that single token instead of starting a new command.
        self.assertEqual(
            logslice.split_pipeline("grep foo; touch marker"),
            [["grep", "foo;", "touch", "marker"]])

    def test_whitespaced_semicolon_stays_in_stage(self):
        # A standalone ';' becomes its own token but is still an ordinary grep
        # argument within the same stage, never a stage/command separator.
        self.assertEqual(
            logslice.split_pipeline("grep foo ; echo hi"),
            [["grep", "foo", ";", "echo", "hi"]])

    def test_quoted_semicolon_stays_literal(self):
        # A ';' inside quotes is part of the grep pattern, one argument.
        self.assertEqual(
            logslice.split_pipeline('grep "foo; touch marker"'),
            [["grep", "foo; touch marker"]])

    def test_and_operator_is_not_a_separator(self):
        # '&&' is not punctuation to the lexer, so it stays an ordinary token
        # inside the grep stage rather than chaining a second command.
        self.assertEqual(
            logslice.split_pipeline("grep foo && wc -l"),
            [["grep", "foo", "&&", "wc", "-l"]])

    def test_or_operator_is_not_a_separator(self):
        # '||' contains '|', but the punctuation_chars lexer emits the run as a
        # single '||' token; split_pipeline only splits on a lone '|', so '||'
        # stays a literal argument and never separates stages.
        self.assertEqual(
            logslice.split_pipeline("grep foo || wc -l"),
            [["grep", "foo", "||", "wc", "-l"]])

    def test_background_operator_is_not_a_separator(self):
        # A single '&' is likewise just a token within the stage.
        self.assertEqual(
            logslice.split_pipeline("grep foo & wc -l"),
            [["grep", "foo", "&", "wc", "-l"]])

    def test_pipe_ampersand_splits_into_rejected_stage(self):
        # '|&' is the only case that does split: the '|' ends the grep stage and
        # the trailing '&' begins a new stage. That stage starts with '&', which
        # is not whitelisted, so validate_pipeline (below) rejects it.
        self.assertEqual(
            logslice.split_pipeline("grep foo |& touch marker"),
            [["grep", "foo"], ["&", "touch", "marker"]])


class ValidatePipelineTest(unittest.TestCase):
    def setUp(self):
        self.ssh = logslice.Ssh("unused")

    def test_accepts_whitelisted(self):
        self.ssh.validate_pipeline([["grep", "x"], ["wc", "-l"]])

    def test_rejects_non_whitelisted_command(self):
        # "true" is a harmless no-op so a validation failure cannot do damage.
        with self.assertRaises(ValueError):
            self.ssh.validate_pipeline([["true"]])

    def test_rejects_non_whitelisted_after_pipe(self):
        with self.assertRaises(ValueError):
            self.ssh.validate_pipeline([["grep", "foo"], ["true"]])

    def test_rejects_empty_stage(self):
        with self.assertRaises(ValueError):
            self.ssh.validate_pipeline([["grep", "foo"], []])

    def test_pipe_ampersand_payload_rejected_by_whitelist(self):
        # 'grep foo |& touch marker' splits into a second stage led by '&', which
        # is not whitelisted -- here the whitelist (not quoting) is the defense.
        stages = logslice.split_pipeline("grep foo |& touch marker")
        with self.assertRaises(ValueError):
            self.ssh.validate_pipeline(stages)

    def test_semicolon_payload_still_passes_whitelist(self):
        # A ';'-injection payload parses into a single grep stage, so it slips
        # past the whitelist untouched -- the whitelist is NOT what stops it.
        # The actual defense is shlex-quoting at assembly time (see
        # SemicolonQuotingTest); this test pins down that validation alone would
        # not catch it, documenting why quoting is mandatory.
        stages = logslice.split_pipeline("grep foo; touch marker")
        self.assertEqual(stages, [["grep", "foo;", "touch", "marker"]])
        self.ssh.validate_pipeline(stages)  # does not raise


# Shell control/redirection operators that must never reach the remote shell as
# operators. Each is neutralized by shlex-quoting (it becomes a literal grep
# argument). '|&' is excluded -- it splits into a stage the whitelist rejects and
# is covered by ValidatePipelineTest instead.
QUOTING_NEUTRALIZED_OPERATORS = [
    "&&", "||", "&", ";", ">", ">>", "<", "`touch x`", "$(touch x)",
]


class SemicolonQuotingTest(unittest.TestCase):
    """Pure tests (no ssh) that the remote command string shlex-quotes every
    token, so an injected ';' (or other operator) is a literal argument and can
    never separate commands on the remote shell."""

    def setUp(self):
        self.ssh = logslice.Ssh("unused")

    def _assemble(self, head_argv, stages):
        # Mirror exactly how run_pipeline builds the remote command string.
        remote = self.ssh._remote_command(head_argv)
        for stage in stages:
            remote += " | " + self.ssh._remote_command(stage)
        return remote

    def test_glued_semicolon_is_quoted(self):
        stages = logslice.split_pipeline("grep foo; touch marker")
        remote = self._assemble(["printf", "x\n"], stages)
        # The ';' survives only inside a single-quoted token; there is no bare
        # '; ' that the shell would read as a command separator.
        self.assertIn("'foo;'", remote)
        self.assertNotIn("foo; ", remote)

    def test_standalone_semicolon_is_quoted(self):
        stages = logslice.split_pipeline("grep foo ; echo hi")
        remote = self._assemble(["printf", "x\n"], stages)
        self.assertIn("';'", remote)

    def test_semicolon_tokens_round_trip_as_literals(self):
        # Re-lexing the assembled command (shlex on whitespace) must give back
        # the exact tokens we put in: each ';'-bearing token stays a single
        # literal argument, so the remote shell sees no command separator.
        head = ["printf", "x\n"]
        stages = logslice.split_pipeline("grep foo; touch marker; echo done")
        remote = self._assemble(head, stages)
        expected = list(head)
        for stage in stages:
            expected.append("|")
            expected.extend(stage)
        self.assertEqual(shlex.split(remote), expected)

    def test_operators_round_trip_as_literals(self):
        # For every neutralized operator, re-lexing the assembled command must
        # return the exact tokens split_pipeline produced. Because each operator
        # token is single-quoted, the remote shell -- which lexes the same way --
        # sees a literal grep argument, not an operator. Defends &&, ||, &, ;,
        # redirections, and command substitution in one sweep.
        head = ["printf", "x\n"]
        for op in QUOTING_NEUTRALIZED_OPERATORS:
            with self.subTest(operator=op):
                stages = logslice.split_pipeline(
                    "grep foo {} touch marker".format(op))
                # The payload stays a single grep stage (nothing else runs).
                self.assertEqual(len(stages), 1)
                self.assertEqual(stages[0][0], "grep")
                remote = self._assemble(head, stages)
                expected = list(head) + ["|"] + stages[0]
                self.assertEqual(shlex.split(remote), expected)


def _info_time(hour, minute):
    """A logslice --info timestamp on the incident day, matching INFO_RE."""
    return "2026-06-19 {:02d}:{:02d}:00,000000".format(hour, minute)


class FakeSsh:
    """In-memory stand-in for Ssh used by the file-selection tests. It answers
    the only two remote calls selection makes: ``ls -1 <dir>`` (directory
    listings) and ``<bin> --info <path>`` (the first/last record timestamps of a
    file). A missing directory behaves like the real ``ls`` under check=False:
    empty output rather than an abort."""

    def __init__(self, listings, info):
        # listings: {directory: [names]}; info: {path: (first(h,m), last(h,m))}
        self._listings = listings
        self._info = info
        self.info_calls = []

    def run(self, argv, capture=True, check=True):
        if argv[:2] == ["ls", "-1"]:
            directory = argv[2]
            if directory not in self._listings:
                if check:
                    raise AssertionError("unexpected ls of " + directory)
                return ""  # missing dir -> empty, like ls 2>/dev/null
            return "".join(name + "\n" for name in self._listings[directory])
        if len(argv) == 3 and argv[1] == "--info":
            path = argv[2]
            self.info_calls.append(path)
            (fh, fm), (lh, lm) = self._info[path]
            return "first: {}\nlast: {}\n".format(
                _info_time(fh, fm), _info_time(lh, lm))
        raise AssertionError("unexpected remote call: {!r}".format(argv))


# A realistic layout: the live `logs` dir holds the two newest (sequence-named)
# rotations, older ones have been moved to the per-day archive. The handoff is at
# 11:00 -- archive ends there, live begins there.
LIVE_DIR = logslice.REMOTE_LOGS_DIR
ARCHIVE_DIR = "/yt/master-logs-archive"
ARCHIVE_DAY = ARCHIVE_DIR + "/2026-06-19"

LIVE_CURRENT = "master.debug.log"               # 11:30 - 12:00 (newest)
LIVE_ROT1 = "master.debug.log.1.zst"            # 11:00 - 11:30
ARCH_EARLY = "master.debug.log.2026-06-19_10-30.zst"  # 10:00 - 10:30
ARCH_LATE = "master.debug.log.2026-06-19_11-00.zst"   # 10:30 - 11:00 (newest archive)


def _make_ssh():
    listings = {
        LIVE_DIR: [
            LIVE_CURRENT, LIVE_ROT1,
            "master.error.log",                 # wrong channel: ignored for debug
            "master.debug.log.1.zst.trindex",   # index sidecar: not a log file
        ],
        ARCHIVE_DIR: ["2026-06-19"],
        ARCHIVE_DAY: [ARCH_EARLY, ARCH_LATE],
    }
    info = {
        LIVE_DIR + "/" + LIVE_CURRENT: ((11, 30), (12, 0)),
        LIVE_DIR + "/" + LIVE_ROT1: ((11, 0), (11, 30)),
        ARCHIVE_DAY + "/" + ARCH_EARLY: ((10, 0), (10, 30)),
        ARCHIVE_DAY + "/" + ARCH_LATE: ((10, 30), (11, 0)),
    }
    return FakeSsh(listings, info)


def _select(ssh, start, end, archive_dir=ARCHIVE_DIR):
    start_time = logslice.parse_user_time(start) if start else None
    end_time = logslice.parse_user_time(end) if end else None
    series = logslice.discover_series(
        ssh, "debug", start_time, end_time, archive_dir)
    selected, _ = logslice.select_log_files(
        ssh, "/tmp/logslice", series, start_time, end_time)
    return selected


def _names_with_dirs(selected):
    return [(f.directory, f.name) for f in selected]


class ArchiveParsingTest(unittest.TestCase):
    def test_parses_archive_filename(self):
        from datetime import datetime
        f = logslice.parse_log_name(ARCH_LATE, ARCHIVE_DAY)
        self.assertIsNotNone(f)
        self.assertEqual(f.base, "master")
        self.assertEqual(f.channel, "debug")
        self.assertEqual(f.timestamp, datetime(2026, 6, 19, 11, 0))
        self.assertFalse(f.is_current)
        self.assertEqual(f.directory, ARCHIVE_DAY)
        self.assertEqual(f.path, ARCHIVE_DAY + "/" + ARCH_LATE)

    def test_default_directory_is_live(self):
        f = logslice.parse_log_name(LIVE_CURRENT)
        self.assertTrue(f.is_current)
        self.assertEqual(f.directory, logslice.REMOTE_LOGS_DIR)

    def test_index_sidecar_rejected(self):
        self.assertIsNone(
            logslice.parse_log_name("master.debug.log.1.zst.trindex", LIVE_DIR))


class ArchiveDayDirsTest(unittest.TestCase):
    def test_keeps_window_days_with_one_day_margin(self):
        names = ["2026-06-17", "2026-06-18", "2026-06-19", "2026-06-20",
                 "2026-06-21", "not-a-day"]
        start = logslice.parse_user_time("2026-06-19 10:00")
        end = logslice.parse_user_time("2026-06-19 11:00")
        # [start-1, end+1] = 06-18 .. 06-20; 06-17 and 06-21 are out, junk dropped.
        self.assertEqual(
            logslice.archive_day_dirs(names, start, end),
            ["2026-06-18", "2026-06-19", "2026-06-20"])

    def test_no_window_keeps_all_days(self):
        names = ["2026-06-18", "2026-06-19"]
        self.assertEqual(
            logslice.archive_day_dirs(names, None, None),
            ["2026-06-18", "2026-06-19"])


class ArchiveDiscoveryTest(unittest.TestCase):
    def test_archive_skipped_without_window(self):
        # An unbounded scan of the whole archive is never wanted.
        ssh = _make_ssh()
        base, files = logslice.discover_archive(
            ssh, "debug", None, None, ARCHIVE_DIR)
        self.assertEqual(files, [])

    def test_archive_disabled(self):
        ssh = _make_ssh()
        start = logslice.parse_user_time("2026-06-19 10:45")
        end = logslice.parse_user_time("2026-06-19 11:15")
        base, files = logslice.discover_archive(
            ssh, "debug", start, end, None)
        self.assertEqual(files, [])

    def test_archive_missing_dir_is_not_fatal(self):
        # Hosts without an archive (the common case) must still work: discover
        # returns empty and live discovery is unaffected.
        ssh = FakeSsh({LIVE_DIR: [LIVE_CURRENT]},
                      {LIVE_DIR + "/" + LIVE_CURRENT: ((11, 30), (12, 0))})
        start = logslice.parse_user_time("2026-06-19 11:40")
        end = logslice.parse_user_time("2026-06-19 11:50")
        base, files = logslice.discover_archive(
            ssh, "debug", start, end, ARCHIVE_DIR)
        self.assertEqual(files, [])


class ArchiveSelectionTest(unittest.TestCase):
    def test_pure_archive_window_selects_only_archive(self):
        selected = _select(_make_ssh(), "2026-06-19 10:05", "2026-06-19 10:25")
        self.assertEqual(_names_with_dirs(selected),
                         [(ARCHIVE_DAY, ARCH_EARLY)])

    def test_pure_live_window_selects_only_live(self):
        selected = _select(_make_ssh(), "2026-06-19 11:40", "2026-06-19 11:55")
        self.assertEqual(_names_with_dirs(selected),
                         [(LIVE_DIR, LIVE_CURRENT)])

    def test_boundary_straddling_window_selects_from_both(self):
        # The window 10:45 - 11:15 spans the 11:00 archive/live handoff: it must
        # pick the tail of the archive AND the head of the live logs, ordered
        # oldest -> newest.
        selected = _select(_make_ssh(), "2026-06-19 10:45", "2026-06-19 11:15")
        self.assertEqual(
            _names_with_dirs(selected),
            [(ARCHIVE_DAY, ARCH_LATE), (LIVE_DIR, LIVE_ROT1)])

    def test_straddling_window_spans_archive_tail_and_live_head(self):
        # A wider straddle: from inside the earliest archive file to inside the
        # current live file pulls every file in between, across both dirs.
        selected = _select(_make_ssh(), "2026-06-19 10:15", "2026-06-19 11:45")
        self.assertEqual(
            _names_with_dirs(selected),
            [(ARCHIVE_DAY, ARCH_EARLY), (ARCHIVE_DAY, ARCH_LATE),
             (LIVE_DIR, LIVE_ROT1), (LIVE_DIR, LIVE_CURRENT)])

    def test_wrong_channel_and_sidecar_ignored(self):
        # The error-channel file and the .trindex sidecar in the live dir must
        # never be selected for a debug query.
        selected = _select(_make_ssh(), "2026-06-19 10:15", "2026-06-19 11:45")
        names = [name for _, name in _names_with_dirs(selected)]
        self.assertNotIn("master.error.log", names)
        self.assertNotIn("master.debug.log.1.zst.trindex", names)


@unittest.skipUnless(_ssh_localhost_works(), "ssh localhost unavailable")
class SshIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.ssh = logslice.Ssh("localhost")

    def test_example_pipeline_runs(self):
        # The documented example "grep Error | wc -l" over a sample log: printf
        # stands in for the logslice binary; two of the three lines match.
        head = ["printf", "info ok\nError one\nError two\n"]
        stages = logslice.split_pipeline("grep Error | wc -l")
        self.assertEqual(
            self.ssh.run_pipeline(head, stages, capture=True).strip(), "2")

    def test_rejects_non_whitelisted(self):
        with self.assertRaises(ValueError):
            self.ssh.run_pipeline(["printf", "x\n"], [["true"]])

    def test_quoting_prevents_injection(self):
        # A shell-metachar-laden grep pattern must be treated as a literal pattern,
        # never executed. grep finds no match, so the injected command cannot run.
        marker = "/tmp/logslice_injection_marker"
        subprocess.run(["rm", "-f", marker])
        head = ["printf", "x\n"]
        stages = [["grep", "y; touch " + marker]]
        self.ssh.run_pipeline(head, stages, capture=False)
        self.assertFalse(os.path.exists(marker),
                         "injected command was executed")

    def test_semicolon_injection_via_pipeline_string(self):
        # End-to-end: a ';' payload typed as a whole pipeline string is parsed by
        # split_pipeline, then run. The 'touch' tokens must reach grep as literal
        # arguments (grep treats them as files to search and finds nothing on the
        # remote side), never as a separate command, so the marker is not created.
        marker = "/tmp/logslice_semicolon_marker"
        subprocess.run(["rm", "-f", marker])
        head = ["printf", "x\n"]
        stages = logslice.split_pipeline("grep foo; touch " + marker)
        self.assertEqual(stages, [["grep", "foo;", "touch", marker]])
        with _silence_fd_output():
            self.ssh.run_pipeline(head, stages, capture=False)
        self.assertFalse(os.path.exists(marker),
                         "injected ';' command was executed")
        subprocess.run(["rm", "-f", marker])

    def test_operator_injection_does_not_run_marker(self):
        # End-to-end over real ssh: operators that WOULD run the RHS in a shell
        # ('||' on grep's no-match failure, '&' backgrounding, and command
        # substitution which always runs) must not create the marker, proving the
        # touch reached grep as a literal argument rather than a chained command.
        marker = "/tmp/logslice_operator_marker"
        head = ["printf", "x\n"]
        payloads = [
            "grep foo || touch " + marker,
            "grep foo & touch " + marker,
            "grep `touch {}`".format(marker),
            "grep $(touch {})".format(marker),
        ]
        for payload in payloads:
            with self.subTest(payload=payload):
                subprocess.run(["rm", "-f", marker])
                stages = logslice.split_pipeline(payload)
                with _silence_fd_output():
                    self.ssh.run_pipeline(head, stages, capture=False)
                self.assertFalse(
                    os.path.exists(marker),
                    "injected command was executed for: " + payload)
        subprocess.run(["rm", "-f", marker])


if __name__ == "__main__":
    unittest.main()
