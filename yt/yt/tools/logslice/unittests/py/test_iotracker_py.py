"""IOTracker routing, retained evidence and synthetic stream contracts."""

import contextlib
import importlib.util
import io
import json
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Union
from unittest import mock


def source_path(relative):
    try:
        import yatest.common
        return Path(yatest.common.source_path(relative))
    except ImportError:
        return Path(__file__).resolve().parents[6] / relative


spec = importlib.util.spec_from_file_location(
    "logslice_iotracker", source_path("yt/yt/tools/logslice/logslice.py"))
logslice = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logslice)
EVIDENCE = json.loads(source_path(
    "yt/yt/tools/logslice/unittests/py/fixtures/ytadminreq_62112/evidence.json").read_text())
HOST = "klg1-7548-dat-node-seneca-klg.klg.yp-c.yandex.net"
BASE = "node-klg1-7548"
START = "2026-11-05 12:00:00"
END = "2026-11-05 12:16:00"
KINDS = ("io_aggr", "io_raw")


def argv(kind="io_aggr", host=HOST, component=BASE, start=START, end=END, extra=()):
    result = [host, "--type", kind]
    for flag, value in (("--component", component), ("-t", start), ("-e", end)):
        if value is not None:
            result += [flag, value]
    return result + list(extra) + ["--", "-F", "read"]


def metadata(stderr):
    return json.loads(stderr.splitlines()[-1])


@dataclass
class SliceResult:
    exit_code: Union[int, str]
    stdout: str
    stderr: str
    read_calls: list
    preflight_call_count: int
    binary_call_count: int
    command_calls: list


class LogHost:
    """A single log root; real discovery and coverage code consume its replies."""

    def __init__(self, files, outputs=None, bounds=(START, END), timezone="+0300"):
        self.files = files
        self.outputs = outputs or {}
        self.bounds = bounds
        self.timezone = timezone

    def run(self, command, **kwargs):
        if command == ["date", "+%z"]:
            return self.timezone + "\n"
        if command == ["ls", "-1", "logs"]:
            return "\n".join(self.files)
        if command[:2] == [logslice.REMOTE_BIN, "--info"]:
            assert command[2] in ["logs/" + name for name in self.files]
            if self.bounds is None:
                return "no timestamped lines\n"
            first, last = self.bounds
            return "first: {},000000\nlast: {},000000\n".format(first, last)
        raise AssertionError("unexpected remote command: {!r}".format(command))

    def read(self, command, stages, **kwargs):
        assert command[-1] in ["logs/" + name for name in self.files]
        return self.outputs.get(command[-1], logslice.PipelineResult(1, 0, "", ""))


class IOTrackerTest(unittest.TestCase):
    def run_slice(self, arguments, backend, access_failure=None):
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(logslice.sys, "argv", ["logslice.py"] + arguments), \
                mock.patch.object(logslice, "ssh_access_preflight", return_value=access_failure) as preflight, \
                mock.patch.object(logslice, "resolve_logslice", return_value="/unused/offline") as binary, \
                mock.patch.object(logslice.Ssh, "copy_binary"), \
                mock.patch.object(logslice.Ssh, "run", side_effect=backend.run) as commands, \
                mock.patch.object(logslice.Ssh, "run_pipeline_result", side_effect=backend.read) as reads, \
                mock.patch.object(logslice.subprocess, "run", side_effect=AssertionError("unexpected subprocess")), \
                contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                code = logslice.main()
            except SystemExit as error:
                code = error.code
        return SliceResult(
            exit_code=code,
            stdout=stdout.getvalue(),
            stderr=stderr.getvalue(),
            read_calls=reads.call_args_list,
            preflight_call_count=preflight.call_count,
            binary_call_count=binary.call_count,
            command_calls=commands.call_args_list,
        )

    def test_retained_rejection_and_info_control(self):
        failure = EVIDENCE["saved105"]
        control = EVIDENCE["saved045"]
        selected = metadata(control["stderr"])["selected_files"][0]
        backend = LogHost([Path(selected).name], bounds=(
            "2026-11-05 12:54:00", "2026-11-05 12:56:00"))
        result = self.run_slice(failure["argv"], backend)
        self.assertEqual((result.exit_code, result.stdout, result.stderr, result.read_calls), (
            failure["exit_code"], failure["stdout"], failure["stderr"], []))
        backend.outputs[selected] = logslice.PipelineResult(0, 0, control["stdout"], "")
        result = self.run_slice(control["argv"], backend)
        self.assertEqual((result.exit_code, result.stdout), (control["exit_code"], control["stdout"]))
        self.assertEqual(metadata(result.stderr), metadata(control["stderr"]))
        self.assertEqual(result.read_calls[0].args[0], [
            logslice.REMOTE_BIN, "-t", "2026-11-05 12:54:00", "-e",
            "2026-11-05 12:56:00", selected])

    def test_types_keep_all_unchanged_and_select_only_exact_channel(self):
        self.assertEqual(logslice.parse_log_types("all"), ["debug", "info", "error"])
        for kind in KINDS:
            with self.subTest(kind=kind):
                self.assertEqual(logslice.parse_log_types(kind), [kind])
                name = BASE + "." + kind + ".yson.log"
                rotations = [name + ".2.zst", name + ".2026-11-05_12-16.gz", name]
                listing = rotations + [
                    name + ".trindex", name + ".zst.trindex", BASE + ".log",
                    "master." + kind + ".yson.log",
                    BASE + ".event_log.yson", BASE + "." + kind + ".yson",
                ]
                base, files = logslice.order_series(
                    [logslice.parse_log_name(item) for item in listing], kind, BASE)
                self.assertEqual(base, BASE)
                self.assertEqual([file.name for file in files], rotations)
                self.assertEqual(logslice.live_log_dir(kind), "logs")
                self.assertIsNone(logslice.parse_log_name(name + ".trindex"))
                self.assertEqual(logslice._candidate_bases(rotations, kind), {BASE})

    def test_real_discovery_forwards_bounds_and_filter_for_both_streams(self):
        for kind in KINDS:
            with self.subTest(kind=kind):
                name = BASE + "." + kind + ".yson.log.2.zstd"
                record = EVIDENCE["synthetic"]["record"]
                if kind == "io_raw":
                    record = record.replace("IOAggregate", "IORaw")
                pipeline_result = logslice.PipelineResult(0, 0, record, "")
                backend = LogHost([name, "master." + kind + ".yson.log", BASE + ".log"], {
                    "logs/" + name: pipeline_result})
                result = self.run_slice(argv(kind), backend)
                self.assertEqual((result.exit_code, result.stdout), (0, record))
                self.assertEqual(metadata(result.stderr)["selected_base"], BASE)
                self.assertEqual(metadata(result.stderr)["selected_files"], ["logs/" + name])
                self.assertEqual(result.read_calls[0].args, (
                    [logslice.REMOTE_BIN, "-t", START, "-e", END, "logs/" + name],
                    [["grep", "-F", "read"]]))
                self.assertFalse(result.read_calls[0].kwargs["access_log"])

    def test_combined_streams_merge_by_formatter_instant(self):
        names = [BASE + "." + kind + ".yson.log" for kind in KINDS]
        record = EVIDENCE["synthetic"]["record"]
        before = record.replace("12:05:00,000000", "12:04:00,9")
        backend = LogHost(names, {
            "logs/" + names[0]: logslice.PipelineResult(0, 0, record, ""),
            "logs/" + names[1]: logslice.PipelineResult(0, 0, before, ""),
        })
        result = self.run_slice(argv("io_aggr,io_raw"), backend)
        self.assertEqual((result.exit_code, result.stdout), (0, before + record))

    def test_local_instants_normalize_precision_before_merging(self):
        long_fraction = '{"instant"="2026-11-05 12:05:00,100000";}\n'
        short_fraction = '{"instant"="2026-11-05 12:05:00,1";}\n'
        outputs = [logslice.PipelineResult(0, 0, text, "")
                   for text in (long_fraction, short_fraction)]
        destination = io.StringIO()
        logslice.write_merged_pipeline_outputs(outputs, destination, parse_yson=True)
        self.assertEqual(destination.getvalue(), long_fraction + short_fraction)

    def test_instant_payload_values_do_not_replace_the_formatter_key(self):
        record = '{"instant"="2026-11-05 12:05:00,000000";"category"="instant";}\n'
        result = logslice.PipelineResult(0, 0, record, "")
        self.assertIsNone(logslice.structured_output_timestamp_error(result, "io_aggr"))
        self.assertEqual(logslice._timestamped_records(record, 0, parse_yson=True)[0][0],
                         "2026-11-05T12:05:00.000000")

    def test_mixed_utc_and_local_bounds_use_the_server_timezone(self):
        name = BASE + ".io_aggr.yson.log"
        record = EVIDENCE["synthetic"]["record"].replace("12:05:00", "10:05:00")
        backend = LogHost([name], {"logs/" + name: logslice.PipelineResult(0, 0, record, "")},
                          bounds=("2026-11-05 10:00:00", "2026-11-05 10:16:00"), timezone="-0500")
        result = self.run_slice(
            argv(start="2026-11-05T15:00:00Z", end="2026-11-05 10:16:00"), backend)
        self.assertEqual((result.exit_code, result.stdout), (0, record), result.stderr)

    def test_unresolved_remote_timezone_does_not_start_discovery(self):
        result = self.run_slice(
            argv(start="2026-11-05T09:00:00Z"), LogHost([], timezone="unknown"))
        self.assertEqual(result.exit_code, 2)
        self.assertFalse(metadata(result.stderr)["log_read"])
        self.assertEqual(result.read_calls, [])
        self.assertEqual([call.args[0] for call in result.command_calls], [["date", "+%z"]])

    def test_absence_requires_readable_files_and_complete_coverage(self):
        for kind in KINDS:
            name = BASE + "." + kind + ".yson.log"
            record = EVIDENCE["synthetic"]["record"]
            for bounds, pipeline_result, expected in (
                    ((START, END), logslice.PipelineResult(1, 0, "", ""), 3),
                    (("2026-11-05 12:01:00", END), logslice.PipelineResult(1, 0, "", ""), 4),
                    (("2026-11-05 12:01:00", END), logslice.PipelineResult(0, 0, record, ""), 4),
                    (None, logslice.PipelineResult(1, 0, "", ""), 4),
                    ((START, END), logslice.PipelineResult(2, 2, "", "zstd decompression failed"), 2)):
                with self.subTest(kind=kind, bounds=bounds, expected=expected):
                    backend = LogHost([name], {"logs/" + name: pipeline_result}, bounds=bounds)
                    result = self.run_slice(argv(kind), backend)
                    self.assertEqual(result.exit_code, expected, result.stderr)
                    self.assertEqual(len(result.read_calls), 1)
                    if expected == 3:
                        self.assertTrue(metadata(result.stderr)["log_read"])
                        self.assertEqual(metadata(result.stderr)["gaps"], [])
            result = self.run_slice(argv(kind), LogHost([]))
            self.assertEqual(result.exit_code, 2)
            self.assertFalse(metadata(result.stderr)["log_read"])
            self.assertEqual(result.read_calls, [])

    def test_missing_or_invalid_formatter_instant_fails(self):
        for kind in KINDS:
            name = BASE + "." + kind + ".yson.log"
            for record in (
                    '{"bytes"=4096;}\n', '0\n',
                    '{"instant"="2026-13-05 12:05:00,000000";}\n',
                    '{"payload"={"instant"="2026-11-05 12:05:00,000000";};}\n',
                    '{"message"="fake \\"instant\\"=\\"2026-11-05 12:05:00,000000\\"";}\n',
                    '{"timestamp"="2026-11-05T09:05:00Z";}\n'):
                with self.subTest(kind=kind, record=record):
                    backend = LogHost([name], {
                        "logs/" + name: logslice.PipelineResult(0, 0, record, "")})
                    result = self.run_slice(argv(kind), backend)
                    self.assertEqual(result.exit_code, 2)
                    self.assertEqual(result.stdout, "")
                    self.assertIn("no valid instant", result.stderr)

    def test_scope_and_bounds_rejected_before_access(self):
        for kind in KINDS:
            cases = [
                {"host": "scheduler-test"}, {"host": "sas1-123-tab-node-test"},
                {"host": "sas1-123-exec-node-test"}, {"host": "sas1-123-node-test"},
                {"component": "master"}, {"component": "/tmp/node"},
                {"component": BASE + "." + kind + ".yson"},
                {"extra": ["--archive-dir", "/tmp/logs"]},
                {"start": None}, {"end": None}, {"start": "garbage"},
                {"end": START, "start": END}, {"end": "2026-13-01T00:00:00Z"},
                {"end": "25:99"},
                {"start": "2026-11-05T12:00:00"},
                {"start": "2026-11-05 12:00:00,0000001"},
                {"kind": kind + ",info"}, {"kind": kind + ",event_log"},
                {"kind": kind + ".yson"}, {"kind": "/tmp/" + kind},
                {"extra": ["-x", "jq -c ."]},
            ]
            for case in cases:
                with self.subTest(kind=kind, case=case):
                    options = dict(kind=kind)
                    options.update(case)
                    result = self.run_slice(argv(**options), LogHost([]))
                    self.assertNotEqual(result.exit_code, 0)
                    self.assertEqual(result.read_calls, [])
                    self.assertEqual(result.preflight_call_count, 0)
                    self.assertEqual(result.binary_call_count, 0)
                    self.assertEqual(result.command_calls, [])

    def test_component_override_requires_exact_discovery(self):
        for kind in KINDS:
            name = BASE + "." + kind + ".yson.log"
            result = self.run_slice(
                argv(kind, component="node-klg1-1111"), LogHost([name]))
            self.assertEqual(result.exit_code, 2)
            self.assertFalse(metadata(result.stderr)["log_read"])
            self.assertEqual(result.read_calls, [])

    def test_authentication_failure_precedes_build_and_discovery(self):
        result = self.run_slice(
            argv(), LogHost([]), access_failure="ssh_noninteractive_check_failed")
        self.assertEqual(result.exit_code, 2)
        self.assertEqual(metadata(result.stderr)["result"], "access_unavailable")
        self.assertFalse(metadata(result.stderr)["log_read"])
        self.assertEqual(result.read_calls, [])
        self.assertEqual(result.binary_call_count, 0)
        self.assertEqual(result.command_calls, [])


if __name__ == "__main__":
    unittest.main()
