import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import yql_docs_validation_executor as executor


DOC_PATH = "yt/docs/ru/yql/reference/example.md"


def query_change(query="SELECT 1 AS value;", expected=None):
    return {
        "id": "example-query",
        "path": DOC_PATH,
        "kind": "query",
        "description": "Validate the documented expression",
        "query": query,
        "expected": expected or {"mode": "rows", "rows": [{"value": 1}]},
    }


def plan(changes):
    return {
        "schema_version": 1,
        "summary": "Validation plan",
        "changes": changes,
    }


class PlanValidationTest(unittest.TestCase):
    def test_accepts_empty_plan_for_empty_diff(self):
        self.assertEqual(executor.validate_plan(plan([]), []), plan([]))

    def test_accepts_one_fenced_json_object_with_prose(self):
        raw = "Validation result:\n```json\n{\"schema_version\": 1}\n```"
        self.assertEqual(executor.parse_plan(raw), {"schema_version": 1})

    def test_rejects_multiple_fenced_json_objects(self):
        raw = "```json\n{}\n```\n```json\n{}\n```"
        with self.assertRaisesRegex(executor.ValidationError, "exactly one JSON object"):
            executor.parse_plan(raw)

    def test_rejects_missing_file_coverage(self):
        with self.assertRaisesRegex(executor.ValidationError, "missing="):
            executor.validate_plan(plan([]), [DOC_PATH])

    def test_rejects_unexpected_file(self):
        with self.assertRaisesRegex(executor.ValidationError, "unexpected="):
            executor.validate_plan(plan([query_change()]), [])

    def test_accepts_multiple_checks_for_one_file(self):
        second = query_change("SELECT 2 AS value;", {"mode": "success"})
        second["id"] = "second-query"
        validated = executor.validate_plan(plan([query_change(), second]), [DOC_PATH])
        self.assertEqual(len(validated["changes"]), 2)

    def test_rejects_extra_schema_keys(self):
        value = plan([])
        value["comment"] = "ignored"
        with self.assertRaisesRegex(executor.ValidationError, r"extra=\['comment'\]"):
            executor.validate_plan(value, [])


class QuerySafetyTest(unittest.TestCase):
    def test_accepts_constant_select(self):
        executor.validate_query("SELECT 1 AS value;")

    def test_accepts_read_below_workspace(self):
        executor.validate_query(
            "USE freud; SELECT * FROM `//home/dev/docs-team/yql-docs/example`;"
        )

    def test_accepts_explicit_freud_table(self):
        executor.validate_query(
            "SELECT * FROM freud.`//home/dev/docs-team/yql-docs/example`;"
        )

    def test_accepts_in_memory_table(self):
        executor.validate_query("SELECT * FROM AS_TABLE(AsList(AsStruct(1 AS value)));")

    def test_rejects_write_before_any_query_runs(self):
        for keyword in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER"):
            with self.subTest(keyword=keyword):
                with self.assertRaises(executor.ValidationError):
                    executor.validate_query(f"{keyword} TABLE foo;")

    def test_rejects_path_outside_workspace(self):
        with self.assertRaisesRegex(executor.ValidationError, "outside"):
            executor.validate_query("SELECT * FROM `//home/another-team/table`;")

    def test_rejects_relative_table(self):
        with self.assertRaisesRegex(executor.ValidationError, "outside"):
            executor.validate_query("SELECT * FROM `Input`;")

    def test_rejects_dynamic_table_source(self):
        with self.assertRaisesRegex(executor.ValidationError, "explicit path"):
            executor.validate_query("SELECT * FROM EACH($paths);")

    def test_rejects_another_cluster(self):
        with self.assertRaisesRegex(executor.ValidationError, "only 'freud'"):
            executor.validate_query("USE hahn; SELECT 1;")

    def test_keywords_inside_string_are_not_statements(self):
        executor.validate_query("SELECT 'INSERT DROP DELETE' AS value;")


class DiffTest(unittest.TestCase):
    def test_reads_merge_base_name_only_diff(self):
        calls = []
        validation_base = "a" * 40

        def fake_runner(args, **kwargs):
            calls.append(args)
            return subprocess.CompletedProcess(args, 0, DOC_PATH + "\n", "")

        paths = executor.get_changed_paths(
            Path("/arcadia"),
            validation_base,
            runner=fake_runner,
        )
        self.assertEqual(paths, [DOC_PATH])
        self.assertEqual(
            calls[0],
            [
                "arc",
                "diff",
                "--name-only",
                validation_base,
                "HEAD",
                "yt/docs/ru/yql",
            ],
        )

    def test_reads_stacked_validation_base_from_commit_message(self):
        validation_base = "b" * 40
        trunk_base = "a" * 40
        calls = []

        def fake_runner(args, **kwargs):
            calls.append(args)
            if args[1] == "log":
                output = json.dumps(
                    [
                        {
                            "message": (
                                "YQL_DOCS_YFM_TO_YT-2\n\n"
                                f"YQL_DOC_SYNC_VALIDATION_BASE: {validation_base}"
                            )
                        }
                    ]
                )
            elif "arcadia/trunk" in args:
                output = trunk_base + "\n"
            else:
                output = validation_base + "\n"
            return subprocess.CompletedProcess(args, 0, output, "")

        actual = executor.get_validation_base(Path("/arcadia"), runner=fake_runner)
        self.assertEqual(actual, validation_base)
        self.assertEqual(calls[0][1], "merge-base")
        self.assertEqual(calls[1][-1], f"{trunk_base}..HEAD")

    def test_falls_back_to_trunk_merge_base_without_metadata(self):
        validation_base = "c" * 40

        def fake_runner(args, **kwargs):
            if args[1] == "log":
                output = "[]"
            else:
                output = validation_base + "\n"
            return subprocess.CompletedProcess(args, 0, output, "")

        actual = executor.get_validation_base(Path("/arcadia"), runner=fake_runner)
        self.assertEqual(actual, validation_base)


class OfflineReportTest(unittest.TestCase):
    def test_marks_query_as_not_executed(self):
        validated = executor.validate_plan(plan([query_change()]), [DOC_PATH])
        report = executor.build_offline_report(validated, [DOC_PATH], "a" * 40)
        self.assertEqual(report["status"], "plan_validated")
        self.assertEqual(report["execution_mode"], "offline")
        self.assertFalse(report["remote_execution"])
        self.assertEqual(report["checks"][0]["status"], "not_executed")
        self.assertNotIn("query_url", report["checks"][0])
        self.assertNotIn("query_id", report["checks"][0])

    def test_rejects_unsafe_query(self):
        safe = query_change()
        unsafe = query_change("DELETE FROM `//home/dev/docs-team/table`;")
        unsafe["id"] = "unsafe-query"
        validated = executor.validate_plan(plan([safe, unsafe]), [DOC_PATH])
        with self.assertRaises(executor.ValidationError):
            executor.build_offline_report(validated, [DOC_PATH], "a" * 40)

    def test_preserves_na_reason(self):
        change = {
            "id": "documentation-only",
            "path": DOC_PATH,
            "kind": "na",
            "description": "Clarify wording",
            "reason": "No executable behavior changed",
        }
        validated = executor.validate_plan(plan([change]), [DOC_PATH])
        report = executor.build_offline_report(validated, [DOC_PATH], "a" * 40)
        self.assertEqual(report["checks"][0]["status"], "na")
        self.assertEqual(report["checks"][0]["reason"], change["reason"])

    def test_reports_no_changes(self):
        validated = executor.validate_plan(plan([]), [])
        report = executor.build_offline_report(validated, [], "a" * 40)
        self.assertEqual(report["status"], "no_changes")
        self.assertFalse(report["remote_execution"])

    def test_writes_compact_report_and_status(self):
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "report.json"
            status_path = Path(directory) / "status"
            report = {"status": "plan_validated", "checks": []}
            executor._write_outputs(report, report_path, status_path)
            self.assertEqual(json.loads(report_path.read_text()), report)
            self.assertEqual(status_path.read_text(), "plan_validated\n")


if __name__ == "__main__":
    unittest.main()
