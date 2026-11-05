import gzip
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ARCADIA_BINARY_PATH = "yt/yt/tools/logslice/bin"
ARCADIA_FIXTURE_PATH = (
    "yt/yt/tools/logslice/unittests/cli/fixtures/local_cli.log"
)


def _find_binary():
    try:
        import yatest.common

        return os.path.join(
            yatest.common.binary_path(ARCADIA_BINARY_PATH),
            "logslice",
        )
    except ImportError:
        directory = Path(__file__).resolve().parent
        while directory != directory.parent:
            candidate = directory / "bin" / "logslice"
            if candidate.is_file():
                return str(candidate)
            directory = directory.parent
        raise RuntimeError("built logslice binary not found")


def _find_fixture():
    try:
        import yatest.common

        return Path(yatest.common.source_path(ARCADIA_FIXTURE_PATH))
    except ImportError:
        return Path(__file__).resolve().parent / "fixtures" / "local_cli.log"


class LocalCliTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.binary = _find_binary()
        cls.fixture = _find_fixture()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.log_file = Path(self.temp_dir.name) / "local_cli.log.gz"
        self.log_file.write_bytes(gzip.compress(self.fixture.read_bytes(), mtime=0))

    def tearDown(self):
        self.temp_dir.cleanup()

    def run_logslice(self, *args):
        return subprocess.run(
            [self.binary, *map(str, args)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def assert_succeeded(self, result):
        self.assertEqual(
            result.returncode,
            0,
            result.stderr.decode("utf-8", errors="replace"),
        )

    def test_free_grep_args_match_grep_string_byte_for_byte(self):
        time_args = (
            "-t", "2026-08-06 13:31:00",
            "-e", "2026-08-06 13:31:07",
        )
        grep_string = self.run_logslice(
            *time_args,
            "-g", "-F 'needle text'",
            self.log_file,
        )
        free_args = self.run_logslice(
            *time_args,
            self.log_file,
            "--", "-F", "needle text",
        )

        self.assert_succeeded(grep_string)
        self.assert_succeeded(free_args)
        self.assertEqual(grep_string.stdout, free_args.stdout)
        self.assertEqual(
            free_args.stdout,
            b"2026-08-06 13:31:01,000000\tI\tTest\tneedle text\n",
        )

    def test_pattern_beginning_with_dash_stays_one_multiword_argument(self):
        result = self.run_logslice(
            self.log_file,
            "--", "-F", "--", "-leading pattern",
        )

        self.assert_succeeded(result)
        self.assertEqual(
            result.stdout,
            b"2026-08-06 13:31:02,000000\tI\tTest\t-leading pattern\n",
        )

    def test_long_grep_option_is_forwarded(self):
        result = self.run_logslice(
            self.log_file,
            "--", "--count", "-F", "needle text",
        )

        self.assert_succeeded(result)
        self.assertEqual(result.stdout, b"1\n")

    def test_empty_match_set_is_successful(self):
        result = self.run_logslice(
            self.log_file,
            "--", "-F", "absent text",
        )

        self.assert_succeeded(result)
        self.assertEqual(result.stdout, b"")

    def test_plain_legacy_grep_argument_still_works_without_delimiter(self):
        result = self.run_logslice(self.log_file, "needle text")

        self.assert_succeeded(result)
        self.assertEqual(
            result.stdout,
            b"2026-08-06 13:31:01,000000\tI\tTest\tneedle text\n",
        )

    def test_tokens_after_delimiter_cannot_change_logslice_options(self):
        result = self.run_logslice(
            self.log_file,
            "--",
            "-F",
            "-e", "--info",
            "-e", "--codec",
            "-e", "--output-file",
            "-e", "-t",
        )

        self.assert_succeeded(result)
        self.assertEqual(
            result.stdout,
            b"2026-08-06 13:31:03,000000\tI\tTest\t--info\n"
            b"2026-08-06 13:31:04,000000\tI\tTest\t--codec\n"
            b"2026-08-06 13:31:05,000000\tI\tTest\t--output-file\n"
            b"2026-08-06 13:31:06,000000\tI\tTest\t-t\n",
        )

    def test_combining_forms_reports_the_final_grep_argv(self):
        result = self.run_logslice(
            "-g", "-F",
            self.log_file,
            "--", "needle text",
        )

        self.assert_succeeded(result)
        self.assertEqual(
            result.stdout,
            b"2026-08-06 13:31:01,000000\tI\tTest\tneedle text\n",
        )
        self.assertIn(
            b'grep argv: "-F" "needle text"',
            result.stderr,
        )

    def test_help_documents_free_grep_arguments(self):
        result = self.run_logslice("--help")

        self.assert_succeeded(result)
        help_text = result.stdout + result.stderr
        self.assertIn(b"log_file", help_text)
        self.assertIn(b"-- GREP_ARGS", help_text)


if __name__ == "__main__":
    unittest.main()
