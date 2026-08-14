"""Unit tests for logslice.py.

The pipeline-parsing/quoting tests are pure and always run. The integration
tests actually shell out over `ssh localhost`; they are skipped automatically
when password-less ssh to localhost is unavailable.
"""

import contextlib
import hashlib
import importlib.util
import io
import os
import shlex
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock


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


class PipelineStatusTest(unittest.TestCase):
    def setUp(self):
        self.ssh = logslice.Ssh("unused")

    def _result(self, returncode, stderr, stdout=None):
        result = mock.Mock()
        result.returncode = returncode
        result.stderr = stderr
        result.stdout = stdout
        return result

    def test_grep_no_match_does_not_fail_logslice(self):
        result = self._result(0, "__LOGSLICE_PIPESTATUS__:0 1\n")
        with mock.patch.object(logslice.subprocess, "run", return_value=result):
            self.assertEqual(
                self.ssh.run_pipeline(["logslice", "file"], [["grep", "x"]]),
                0,
            )

    def test_head_failure_is_reported_with_stderr(self):
        result = self._result(
            1,
            "cannot decode block\n__LOGSLICE_PIPESTATUS__:1 1\n",
        )
        stderr = io.StringIO()
        with mock.patch.object(logslice.subprocess, "run", return_value=result), \
                contextlib.redirect_stderr(stderr):
            self.assertEqual(
                self.ssh.run_pipeline(["logslice", "file"], [["grep", "x"]]),
                1,
            )
        self.assertIn("cannot decode block", stderr.getvalue())
        self.assertNotIn("PIPESTATUS", stderr.getvalue())

    def test_non_grep_filter_failure_is_preserved(self):
        result = self._result(0, "__LOGSLICE_PIPESTATUS__:0 1\n")
        with mock.patch.object(logslice.subprocess, "run", return_value=result):
            self.assertEqual(
                self.ssh.run_pipeline(["logslice", "file"], [["wc", "-x"]]),
                1,
            )


class ParseUserTimeTest(unittest.TestCase):
    """Covers every format parse_user_time accepts. These mirror the formats of
    logslice/lib/time_parser.h, since logslice.py forwards the raw -t/-e string
    to the remote logslice binary -- any format accepted here must round-trip
    through that C++ parser too."""

    def setUp(self):
        self.datetime = datetime

    def test_now_is_recent(self):
        now = logslice.parse_user_time("now")
        self.assertIsNotNone(now)
        self.assertLess(abs(datetime.now() - now), timedelta(seconds=5))

    def test_time_of_day_uses_today(self):
        today = self.datetime.now().date()
        hm = logslice.parse_user_time("14:30")
        self.assertEqual((hm.year, hm.month, hm.day), (today.year, today.month, today.day))
        self.assertEqual((hm.hour, hm.minute, hm.second), (14, 30, 0))
        hms = logslice.parse_user_time("12:23:34")
        self.assertEqual((hms.hour, hms.minute, hms.second), (12, 23, 34))

    def test_bare_date_is_midnight(self):
        self.assertEqual(logslice.parse_user_time("2026-06-19"),
                         self.datetime(2026, 6, 19, 0, 0, 0))

    def test_date_and_hour(self):
        self.assertEqual(logslice.parse_user_time("2026-06-19 15"),
                         self.datetime(2026, 6, 19, 15, 0, 0))

    def test_date_hour_minute(self):
        self.assertEqual(logslice.parse_user_time("2026-06-19 15:52"),
                         self.datetime(2026, 6, 19, 15, 52, 0))

    def test_full_timestamp(self):
        self.assertEqual(logslice.parse_user_time("2018-11-09 05:10:43"),
                         self.datetime(2018, 11, 9, 5, 10, 43))

    def test_subsecond_comma_and_dot(self):
        self.assertEqual(logslice.parse_user_time("2026-06-18 06:00:10,246995"),
                         self.datetime(2026, 6, 18, 6, 0, 10, 246995))
        self.assertEqual(logslice.parse_user_time("2026-06-18 06:00:10.246995"),
                         self.datetime(2026, 6, 18, 6, 0, 10, 246995))

    def test_subsecond_is_right_padded(self):
        # "5" -> 500000 us, "246" -> 246000 us, matching the C++ parser.
        self.assertEqual(logslice.parse_user_time("2026-06-18 06:00:10,5"),
                         self.datetime(2026, 6, 18, 6, 0, 10, 500000))
        self.assertEqual(logslice.parse_user_time("2026-06-18 06:00:10,246"),
                         self.datetime(2026, 6, 18, 6, 0, 10, 246000))

    def test_web_ui_format(self):
        # Same instant as the equivalent full local timestamp.
        self.assertEqual(logslice.parse_user_time("16 Nov 2018 13:56:14"),
                         logslice.parse_user_time("2018-11-16 13:56:14"))

    def test_web_ui_month_is_case_insensitive(self):
        self.assertEqual(logslice.parse_user_time("5 jan 2020 00:00:01"),
                         self.datetime(2020, 1, 5, 0, 0, 1))

    def test_iso_utc_is_converted_to_local(self):
        # 2019-09-19T11:46:04.848360Z as a UTC-aware instant, then made naive
        # local; comparing the epoch is timezone-independent.
        got = logslice.parse_user_time("2019-09-19T11:46:04.848360Z")
        expected_utc = datetime(2019, 9, 19, 11, 46, 4, 848360, tzinfo=timezone.utc)
        self.assertEqual(got.replace(tzinfo=None),
                         expected_utc.astimezone().replace(tzinfo=None))

    def test_partial_fields_are_rejected(self):
        # Half-written time fields and a fraction without seconds are not parsed
        # (parse_user_time returns None so the caller falls back to a full scan).
        for text in ["2026-06-19 1", "2026-06-19 15:5", "2026-06-19 15:52:3",
                     "2026-06-19 15:52.5"]:
            with self.subTest(text=text):
                self.assertIsNone(logslice.parse_user_time(text))

    def test_unparseable_returns_none(self):
        for text in ["garbage", "", "   "]:
            with self.subTest(text=text):
                self.assertIsNone(logslice.parse_user_time(text))

    def test_out_of_range_date_returns_none(self):
        # A well-formed but invalid calendar date is reported as unrecognised
        # rather than raising.
        self.assertIsNone(logslice.parse_user_time("2026-13-40"))


class SplitTimeRangeTest(unittest.TestCase):
    """A single -t value may carry the whole window as "start - end", split on a
    space-padded hyphen so the bare hyphens inside dates/times are untouched."""

    def test_full_timestamp_range_is_split(self):
        self.assertEqual(
            logslice.split_time_range(
                "2026-06-15 11:27:18 - 2026-06-15 11:28:43"),
            ("2026-06-15 11:27:18", "2026-06-15 11:28:43"))

    def test_time_of_day_range_is_split(self):
        self.assertEqual(
            logslice.split_time_range("11:27 - 11:28"),
            ("11:27", "11:28"))

    def test_bare_date_in_range_keeps_internal_hyphens(self):
        self.assertEqual(
            logslice.split_time_range("2026-06-15 - 2026-06-16"),
            ("2026-06-15", "2026-06-16"))

    def test_single_value_is_unchanged(self):
        # No " - " separator: the value passes through untouched, end is None.
        self.assertEqual(
            logslice.split_time_range("2026-06-15 11:27:18"),
            ("2026-06-15 11:27:18", None))

    def test_bare_date_alone_is_not_a_range(self):
        # The date's internal hyphens have no surrounding spaces, so it is not
        # mistaken for a "start - end" window.
        self.assertEqual(
            logslice.split_time_range("2026-06-15"),
            ("2026-06-15", None))

    def test_only_first_separator_splits(self):
        # split is anchored to the first " - "; a stray separator in the end part
        # stays inside the end value (logslice then reports it as unparseable).
        self.assertEqual(
            logslice.split_time_range("11:27 - 11:28 - 11:29"),
            ("11:27", "11:28 - 11:29"))


class FileMd5Test(unittest.TestCase):
    """_file_md5 is the local-side of the binary-reuse check."""

    def test_stable_hex(self):
        content = b"logslice binary contents\n"
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            path = f.name
        try:
            md5 = logslice._file_md5(path)
            self.assertRegex(md5, r"^[0-9a-f]{32}$")
            self.assertEqual(md5, hashlib.md5(content).hexdigest())  # matches stdlib
            self.assertEqual(md5, logslice._file_md5(path))  # idempotent
        finally:
            os.unlink(path)

    def test_changes_with_content(self):
        with tempfile.NamedTemporaryFile(delete=False) as fa:
            fa.write(b"a")
            pa = fa.name
        with tempfile.NamedTemporaryFile(delete=False) as fb:
            fb.write(b"b")
            pb = fb.name
        try:
            self.assertNotEqual(logslice._file_md5(pa), logslice._file_md5(pb))
        finally:
            os.unlink(pa)
            os.unlink(pb)


class _RecordingSsh(logslice.Ssh):
    """An Ssh that records remote calls and answers md5sum/run from a script,
    without touching the network. Used to test copy_binary's md5-skip + the
    cat-based copy path."""
    def __init__(self, remote_md5_value):
        super().__init__("unused")
        self._remote_md5_value = remote_md5_value
        self.calls = []
        self.copies = []  # list of (local_path, remote_path) passed to _copy_via_ssh

    def run(
            self,
            argv,
            capture=True,
            check=True,
            warn_on_error=False,
            retries=0,
            retry_delay=1.0,
            retry_on_empty=False):
        self.calls.append(argv)
        if argv[:2] == ["sh", "-c"] and "md5sum" in argv[2]:
            remote_path = argv[-1]
            return (self._remote_md5_value + "  " + remote_path + "\n"
                    if self._remote_md5_value else "")
        return ""

    def _copy_via_ssh(self, local_path, remote_path):
        # stub the actual network copy — just record that it was attempted.
        self.copies.append((local_path, remote_path))


class CopyBinaryTest(unittest.TestCase):
    """copy_binary reuses the ssh socket (cat, not scp) and skips the copy when
    the remote md5 already matches the local binary (the YTADMIN-13042 fix)."""

    def _local_bin(self, content=b"binary v1"):
        import tempfile
        f = tempfile.NamedTemporaryFile(delete=False)
        f.write(content)
        f.close()
        return f.name

    def test_skips_copy_when_remote_md5_matches(self):
        local = self._local_bin()
        try:
            local_md5 = logslice._file_md5(local)
            ssh = _RecordingSsh(remote_md5_value=local_md5)
            ssh.copy_binary(local, "/tmp/logslice")
            self.assertEqual(ssh.calls[0][-1], "/tmp/logslice")
            self.assertEqual(ssh.copies, [])
        finally:
            os.unlink(local)

    def test_attempts_copy_when_remote_differs(self):
        local = self._local_bin(b"binary v1")
        try:
            ssh = _RecordingSsh(remote_md5_value="0" * 32)  # different md5
            ssh.copy_binary(local, "/tmp/logslice")
            self.assertEqual(ssh.calls[0][-1], "/tmp/logslice")
            self.assertEqual(ssh.copies, [(local, "/tmp/logslice")])
        finally:
            os.unlink(local)

    def test_copy_when_remote_file_absent(self):
        local = self._local_bin(b"binary v1")
        try:
            ssh = _RecordingSsh(remote_md5_value=None)  # md5sum returns ""
            ssh.copy_binary(local, "/tmp/logslice")
            # remote_md5 returns None -> must copy.
            self.assertEqual(ssh.copies, [(local, "/tmp/logslice")])
        finally:
            os.unlink(local)


class AtomicCopyTest(unittest.TestCase):
    def test_upload_uses_temporary_file_and_atomic_replace(self):
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=b"", stderr=b"")
        ssh = logslice.Ssh("unused")
        with tempfile.NamedTemporaryFile() as local:
            with mock.patch.object(
                    logslice.subprocess, "run", return_value=completed) as run:
                ssh._copy_via_ssh(local.name, "/tmp/logslice")

        command = run.call_args.args[0][-1]
        self.assertIn('cat > "$tmp"', command)
        self.assertIn('chmod +x "$tmp"', command)
        self.assertIn('mv -f -- "$tmp" /tmp/logslice', command)
        self.assertNotIn("cat > /tmp/logslice", command)


class RemoteMd5Test(unittest.TestCase):
    def test_only_reuses_executable_file(self):
        ssh = _RecordingSsh(remote_md5_value="abcdef0123456789abcdef0123456789")
        ssh.remote_md5("/tmp/logslice")
        self.assertIn('[ -x "$1" ]', ssh.calls[0][2])

    def test_parses_md5_token(self):
        ssh = _RecordingSsh(remote_md5_value="abcdef0123456789abcdef0123456789")
        self.assertEqual(ssh.remote_md5("/tmp/logslice"),
                         "abcdef0123456789abcdef0123456789")

    def test_returns_none_when_absent(self):
        # md5sum on a missing file prints nothing to stdout (check=False) -> None.
        ssh = _RecordingSsh(remote_md5_value=None)
        self.assertIsNone(ssh.remote_md5("/tmp/missing"))

    def test_rejects_non_hex(self):
        ssh = _RecordingSsh(remote_md5_value="not-an-md5")
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertIsNone(ssh.remote_md5("/tmp/logslice"))

    def test_warns_on_unparseable_md5_output(self):
        # md5sum exits 0 but prints a non-hex token (corrupted binary / wrong
        # tool): remote_md5 returns None but must WARN, not stay silent.
        ssh = _RecordingSsh(remote_md5_value="garbage-not-hex-at-all")
        # _RecordingSsh.run returns the stored string with exit 0; remote_md5
        # must surface a warning about the unparseable output.
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            self.assertIsNone(ssh.remote_md5("/tmp/logslice"))
        self.assertIn("unparseable output", buf.getvalue())


class SshRunTest(unittest.TestCase):
    """Guard against silent error swallowing: every non-zero exit / probe
    failure must surface its stderr, never return empty as if nothing happened."""

    @staticmethod
    def _result(returncode, stdout="", stderr=""):
        return subprocess.CompletedProcess(
            args=[], returncode=returncode, stdout=stdout, stderr=stderr)

    def test_run_check_false_surfaces_stderr_warning(self):
        ssh = logslice.Ssh("unused")
        result = self._result(1, stderr="md5sum: /tmp/x: Permission denied")
        buf = io.StringIO()
        with mock.patch.object(logslice.subprocess, "run", return_value=result):
            with contextlib.redirect_stderr(buf):
                out = ssh.run(
                    ["md5sum", "/tmp/x"],
                    check=False,
                    capture=True,
                    warn_on_error=True)
        self.assertEqual(out, "")                      # non-fatal: empty stdout
        self.assertIn("Permission denied", buf.getvalue())  # but stderr surfaced
        self.assertIn("non-fatal", buf.getvalue())

    def test_run_check_false_silent_by_default(self):
        # The common case (ls of a missing archive dir) must stay silent: a
        # non-zero exit with stderr is NOT warned unless warn_on_error=True, so
        # a clean run isn't polluted with "No such file or directory" noise.
        ssh = logslice.Ssh("unused")
        result = self._result(
            2, stderr="ls: cannot access '/x': No such file or directory")
        buf = io.StringIO()
        with mock.patch.object(logslice.subprocess, "run", return_value=result):
            with contextlib.redirect_stderr(buf):
                out = ssh.run(["ls", "-1", "/x"], check=False, capture=True)
        self.assertEqual(out, "")
        self.assertEqual(buf.getvalue(), "")            # silent by default

    def test_run_check_false_reports_failure_without_stderr(self):
        # A non-zero exit is still visible when the remote command supplied no
        # diagnostic text of its own.
        ssh = logslice.Ssh("unused")
        result = self._result(2)
        buf = io.StringIO()
        with mock.patch.object(logslice.subprocess, "run", return_value=result):
            with contextlib.redirect_stderr(buf):
                out = ssh.run(
                    ["ls", "-1", "/no/such/dir"],
                    check=False,
                    capture=True,
                    warn_on_error=True)
        self.assertEqual(out, "")
        self.assertIn("no stderr", buf.getvalue())

    def test_retry_always_warns_on_total_failure(self):
        # Even without --verbose, a probe that fails every retry must print why
        # (not swallow the stderr). Regression for the verbose-gated message.
        ssh = logslice.Ssh("unused")
        results = [self._result(1, stderr="Broken pipe") for _ in range(3)]
        buf = io.StringIO()
        with mock.patch.object(
                logslice.subprocess, "run", side_effect=results) as run:
            with contextlib.redirect_stderr(buf):
                out = ssh.run(
                    ["logslice", "--info", "/p"],
                    check=False,
                    warn_on_error=True,
                    retries=2,
                    retry_delay=0,
                    retry_on_empty=True)
        self.assertEqual(out, "")
        self.assertEqual(run.call_count, 3)
        msg = buf.getvalue()
        self.assertIn("after 3 attempt(s)", msg)
        self.assertIn("Broken pipe", msg)

    def test_retry_quiet_after_transient_failures(self):
        ssh = logslice.Ssh("unused")
        results = [
            self._result(1, stderr="Broken pipe"),
            self._result(0, stdout=""),
            self._result(0, stdout="first: x\nlast: y\n"),
        ]
        buf = io.StringIO()
        with mock.patch.object(
                logslice.subprocess, "run", side_effect=results) as run:
            with contextlib.redirect_stderr(buf):
                out = ssh.run(
                    ["logslice", "--info", "/p"],
                    check=False,
                    warn_on_error=True,
                    retries=2,
                    retry_delay=0,
                    retry_on_empty=True)
        self.assertEqual(out, "first: x\nlast: y\n")
        self.assertEqual(run.call_count, 3)
        self.assertEqual(buf.getvalue(), "")

    def test_streaming_command_rejects_retries(self):
        ssh = logslice.Ssh("unused")
        with self.assertRaisesRegex(ValueError, "capture=True"):
            ssh.run(["true"], capture=False, retries=1)


class SshOptionsTest(unittest.TestCase):
    """SSH socket and timeout settings are configurable with stable defaults."""

    def test_skill_mcp_environment_supplies_shared_long_lived_socket(self):
        with mock.patch.dict(os.environ, {
                logslice.CONTROL_PATH_ENV: "/tmp/shared/%C",
                logslice.CONTROL_PERSIST_ENV: "86400"}):
            ssh = logslice.Ssh("h")
        self.assertEqual(ssh._control_path, "/tmp/shared/%C")
        self.assertIn("ControlPersist=86400", ssh._base_opts)

    def test_uses_supplied_control_socket(self):
        ssh = logslice.Ssh("h", control_socket="/tmp/socket")
        self.assertIn("ControlPath=/tmp/socket", ssh._base_opts)

    def test_connect_probes_even_when_socket_path_exists(self):
        ssh = logslice.Ssh("h", control_socket="/tmp/socket")
        with mock.patch.object(ssh, "run") as run:
            with contextlib.redirect_stderr(io.StringIO()):
                ssh.connect()
        run.assert_called_once_with(["true"])

    def test_default_socket_and_timeouts(self):
        ssh = logslice.Ssh("h")
        self.assertIn("%r@%h:%p", ssh._control_path)
        self.assertIn(
            "ControlPersist={}".format(logslice.CONTROL_PERSIST_DEFAULT),
            ssh._base_opts)
        self.assertIn(
            "ConnectTimeout={}".format(logslice.CONNECT_TIMEOUT_DEFAULT),
            ssh._base_opts)

    def test_custom_timeouts(self):
        ssh = logslice.Ssh("h", control_persist=90, connect_timeout=7)
        self.assertIn("ControlPersist=90", ssh._base_opts)
        self.assertIn("ConnectTimeout=7", ssh._base_opts)


class _InfoSsh:
    def __init__(self, output):
        self.output = output
        self.calls = []

    def run(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        return self.output


class InfoFallbackTest(unittest.TestCase):
    def test_info_returns_none_none_on_probe_failure_without_abort(self):
        # A FileSelector whose --info probe always fails: info() must return
        # (None, None) and not sys.exit, so the selector falls back to hints.
        files = [logslice.LogFile(
            name="m.debug.log.2026-06-19_11-00.zst", base="m", channel="debug",
            rotation=".2026-06-19_11-00.zst", directory="/d")]
        ssh = _InfoSsh("")
        sel = logslice.FileSelector(ssh, "/tmp/logslice", files)
        warning = io.StringIO()
        with contextlib.redirect_stderr(warning):
            first, last = sel.info(0)
        self.assertIsNone(first)
        self.assertIsNone(last)
        self.assertIn("falling back", warning.getvalue())
        self.assertEqual(ssh.calls[0][1]["retries"], 2)
        self.assertTrue(ssh.calls[0][1]["retry_on_empty"])

    def test_info_parses_when_probe_succeeds(self):
        files = [logslice.LogFile(
            name="m.debug.log.2026-06-19_11-00.zst", base="m", channel="debug",
            rotation=".2026-06-19_11-00.zst", directory="/d")]
        ssh = _InfoSsh(
            "first: 2026-06-19 10:30:00,000000\n"
            "last: 2026-06-19 11:00:00,000000\n")
        sel = logslice.FileSelector(ssh, "/tmp/logslice", files)
        first, last = sel.info(0)
        self.assertEqual(first, datetime(2026, 6, 19, 10, 30))
        self.assertEqual(last, datetime(2026, 6, 19, 11, 0))


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

    def run(
            self,
            argv,
            capture=True,
            check=True,
            warn_on_error=False,
            retries=0,
            retry_delay=1.0,
            retry_on_empty=False):
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


class ComponentRoutingTest(unittest.TestCase):
    def test_tablet_node_hostname_maps_to_node_base(self):
        self.assertEqual(
            logslice.infer_host_component(
                "sas5-5383-tab-node-ada.sas.yp-c.yandex.net"
            ),
            ("tablet-node", "node"),
        )

    def test_master_hostname_maps_to_master_base(self):
        self.assertEqual(
            logslice.infer_host_component("m001-zeno.vla.yp-c.yandex.net"),
            ("master", "master"),
        )

    def test_master_cache_hostname_maps_to_master_cache_base(self):
        self.assertEqual(
            logslice.infer_host_component(
                "master-cache-0a42-zeno-9d1f.vla.yp-c.yandex.net"
            ),
            ("master-cache", "master-cache"),
        )

    def test_rpc_proxy_hostname_maps_to_rpc_proxy_base(self):
        self.assertEqual(
            logslice.infer_host_component(
                "vla0-0261-flow-dev-003-rpc-zeno.vla.yp-c.yandex.net"
            ),
            ("rpc-proxy", "proxy-vla0-0261"),
        )

    def test_http_proxy_hostname_maps_to_http_proxy_base(self):
        self.assertEqual(
            logslice.infer_host_component(
                "vla0-6979-proxy-zeno.vla.yp-c.yandex.net"
            ),
            ("http-proxy", "proxy-vla0-6979"),
        )

    def test_clock_hostname_maps_to_clock_base(self):
        self.assertEqual(
            logslice.infer_host_component(
                "clock01-pythia.sas.yp-c.yandex.net"
            ),
            ("clock", "clock"),
        )

    def test_master_candidate_selection_beats_more_numerous_sidecar(self):
        parsed = [
            logslice.parse_log_name("master-vla2-1217.debug.log"),
            logslice.parse_log_name("push-client.debug.log"),
            logslice.parse_log_name("push-client.debug.log.1.zst"),
            logslice.parse_log_name("push-client.debug.log.2.zst"),
        ]
        route = logslice.resolve_component_route(
            "m001-zeno.vla.yp-c.yandex.net",
            None,
            ["master-vla2-1217", "push-client"],
        )
        base, files = logslice.order_series(
            parsed, "debug", route["base"]
        )
        self.assertEqual(route["component"], "master")
        self.assertEqual(route["base"], "master-vla2-1217")
        self.assertEqual(base, "master-vla2-1217")
        self.assertEqual(
            [item.name for item in files], ["master-vla2-1217.debug.log"]
        )

    def test_push_client_has_lower_fallback_priority(self):
        parsed = [
            logslice.parse_log_name("node.debug.log"),
            logslice.parse_log_name("push-client.debug.log"),
            logslice.parse_log_name("push-client.debug.log.1.zst"),
        ]
        base, _ = logslice.order_series(parsed, "debug")
        self.assertEqual(base, "node")

    def test_rpc_proxy_route_uses_deployed_log_base(self):
        route = logslice.resolve_component_route(
            "vla0-0261-flow-dev-003-rpc-zeno.vla.yp-c.yandex.net",
            None,
            ["proxy-vla0-0261", "push-client"],
        )
        self.assertEqual(route["component"], "proxy-vla0-0261")
        self.assertEqual(route["base"], "proxy-vla0-0261")

    def test_explicit_component_takes_precedence(self):
        route = logslice.resolve_component_route(
            "m001-zeno.vla.yp-c.yandex.net",
            "push-client",
            ["master-vla2-1217", "push-client"],
        )
        self.assertEqual(route["component"], "push-client")
        self.assertEqual(route["base"], "push-client")
        self.assertEqual(route["source"], "--component")

    def test_unknown_hostname_fails_with_candidates(self):
        with self.assertRaisesRegex(ValueError, "master, push-client"):
            logslice.resolve_component_route(
                "mystery-pod.sas.yp-c.yandex.net",
                None,
                ["push-client", "master"],
            )

    def test_metadata_names_route_and_roots(self):
        route = logslice.resolve_component_route(
            "m001-zeno.vla.yp-c.yandex.net", None, ["master-vla2-1217"]
        )
        self.assertEqual(
            logslice.routing_metadata(route, ["logs", "/archive/2026-08-05"]),
            [
                "Log routing: role=master component=master "
                "base=master-vla2-1217 source=hostname confidence=high",
                "Resolved log roots: logs, /archive/2026-08-05",
            ],
        )

    def test_master_archive_is_only_used_for_master_routes(self):
        self.assertTrue(logslice.should_use_master_archive(
            "m001-zeno.vla.yp-c.yandex.net", None))
        self.assertFalse(logslice.should_use_master_archive(
            "sas5-5383-tab-node-ada.sas.yp-c.yandex.net", None))
        self.assertFalse(logslice.should_use_master_archive(
            "master-cache-0a42-zeno-9d1f.vla.yp-c.yandex.net", None))


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
