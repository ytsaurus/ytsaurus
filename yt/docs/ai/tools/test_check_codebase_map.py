#!/usr/bin/env python3
"""Regression tests for check_codebase_map.py."""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import check_codebase_map as checker


class CodebaseMapValidatorTest(unittest.TestCase):
    def test_standalone_scoped_looking_path_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            self._touch(repo_root / "yt/yt/server/http_proxy/config.h")

            errors, checked = checker.check_paths("Wrong Arcadia path: `yt/server/http_proxy/config.h`.", repo_root)

        self.assertEqual(checked, 0)
        self.assertEqual(len(errors), 1)
        self.assertIn("yt/server/http_proxy/config.h", errors[0])

    def test_exact_paths_are_extracted_as_full_code_spans(self) -> None:
        markdown = (
            "`yt/yt/server/http_proxy/config.h+typo` " "`yt/docker/jupyter-tutorial/tutorial/About this demo.ipynb`"
        )

        self.assertEqual(
            checker.extract_arcadia_paths(markdown),
            [
                "yt/docker/jupyter-tutorial/tutorial/About this demo.ipynb",
                "yt/yt/server/http_proxy/config.h+typo",
            ],
        )

    def test_only_single_line_inline_spans_declare_exact_paths(self) -> None:
        markdown = (
            "Broken: ```yt/not_present```\n"
            "````text\n"
            "# Arcadia path: yt/also_not_present\n"
            "./ya tool python3 yt/docs/tool.py \\\n"
            "````\n"
            "~~~text\n"
            "yt/tilde_fence_is_also_an_example\n"
            "~~~\n"
        )

        self.assertEqual(checker.extract_arcadia_paths(markdown), ["yt/not_present"])

    def test_fenced_command_examples_do_not_declare_exact_paths(self) -> None:
        markdown = (
            "```bash\n"
            "prefix \"yt/existing\"+typo\n"
            "yt/not_present yt/**\n"
            "echo yt/not_present && ../ya tool ast-index outline yt/server/http_proxy/config.h\n"
            "```\n"
        )

        paths, errors = checker.extract_arcadia_path_candidates(markdown)

        self.assertEqual(paths, [])
        self.assertEqual(errors, [])

    def test_multiline_spans_and_nested_fences_fail_closed(self) -> None:
        invalid_markdown = [
            "`\nyt/not_present\n`",
            "> ```text\n> yt/not_present\n> ```",
            "> ~~~text\n> yt/not_present\n> ~~~",
        ]
        for markdown in invalid_markdown:
            with self.subTest(markdown=markdown):
                errors, checked = checker.check_paths(markdown, Path("/not-used"))
                self.assertTrue(errors)
                self.assertEqual(checked, 0)

    def test_bare_arcadia_root_is_not_counted_as_exact_path(self) -> None:
        self.assertEqual(checker.extract_arcadia_paths("Root: `yt/`"), [])

    def test_live_map_declares_62_concrete_exact_paths(self) -> None:
        map_path = Path(__file__).resolve().parents[1] / "artifacts/kb/codebase-map.md"
        paths = checker.extract_arcadia_paths(map_path.read_text(encoding="utf-8"))
        concrete_paths = [path for path in paths if not checker.validate_pattern_syntax(path)[0]]

        self.assertEqual(len(paths), 65)
        self.assertEqual(len(concrete_paths), 62)
        self.assertNotIn("yt/", paths)

    def test_valid_exact_paths_include_spaces_and_directory_slashes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            self._touch(repo_root / "yt/ya.make")
            self._touch(repo_root / "yt/docker/jupyter-tutorial/tutorial/About this demo.ipynb")
            (repo_root / "yt/directory").mkdir()
            markdown = "`yt/ya.make` " "`yt/docker/jupyter-tutorial/tutorial/About this demo.ipynb` " "`yt/directory/`"

            errors, checked = checker.check_paths(markdown, repo_root)

        self.assertEqual(errors, [])
        self.assertEqual(checked, 3)

    def test_valid_tracked_path_may_contain_punctuation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            path = "yt/docker/tutorial/6. ML workshop (RU)/Workshop.ipynb"
            self._touch(repo_root / path)

            errors, checked = checker.check_paths(f"`{path}`", repo_root)

        self.assertEqual(errors, [])
        self.assertEqual(checked, 1)

    def test_noncanonical_and_suffix_exact_paths_are_rejected(self) -> None:
        invalid_paths = [
            "yt/ya.make+typo",
            "yt/ya.make.",
            "yt/ya.make#typo",
            "yt/ya.make:typo",
            "yt/../ya.make",
            "yt/../../../etc/passwd",
            "yt//ya.make",
            "yt/./ya.make",
            "yt/ya.make/",
            " yt/ya.make",
            "yt/ya.make ",
        ]
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            self._touch(repo_root / "yt/ya.make")
            for path in invalid_paths:
                with self.subTest(path=path):
                    errors, checked = checker.check_paths(f"`{path}`", repo_root)
                    self.assertTrue(errors)
                    self.assertEqual(checked, 0)

    def test_exact_path_cannot_escape_through_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo_root = root / "repo"
            outside = root / "outside.txt"
            outside.write_text("outside", encoding="utf-8")
            (repo_root / "yt").mkdir(parents=True)
            (repo_root / "yt/link").symlink_to(outside)

            errors, checked = checker.check_paths("`yt/link`", repo_root)

        self.assertEqual(checked, 0)
        self.assertEqual(len(errors), 1)
        self.assertIn("escapes", errors[0])

    def test_symlink_loop_is_reported_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            (repo_root / "yt").mkdir()
            loop = repo_root / "yt/loop"
            loop.symlink_to(loop)

            errors, checked = checker.check_paths("`yt/loop`", repo_root)

        self.assertEqual(checked, 0)
        self.assertEqual(len(errors), 1)
        self.assertNotIn("Traceback", errors[0])

    def test_placeholder_and_glob_paths_are_skipped_after_syntax_check(self) -> None:
        markdown = "`yt/<subsystem>/` `yt/**/ya.make` `yt/component/[name].cpp` " "`yt/{one,two}/file?.cpp`"

        errors, checked = checker.check_paths(markdown, Path("/not-used"))

        self.assertEqual(errors, [])
        self.assertEqual(checked, 0)

    def test_unbalanced_or_invalid_placeholder_is_rejected(self) -> None:
        invalid_paths = [
            "yt/not_present[",
            "yt/not_present]",
            "yt/<>",
            "yt/{one,,two}",
            "yt/[nested<name>]",
        ]
        for path in invalid_paths:
            with self.subTest(path=path):
                errors, checked = checker.check_paths(f"`{path}`", Path("/not-used"))
                self.assertTrue(errors)
                self.assertEqual(checked, 0)

    def test_placeholder_does_not_hide_noncanonical_components(self) -> None:
        errors, checked = checker.check_paths("`yt/../<subsystem>/`", Path("/not-used"))

        self.assertEqual(checked, 0)
        self.assertEqual(len(errors), 1)

    def test_pattern_cannot_hide_a_neighboring_path_in_one_span(self) -> None:
        errors, checked = checker.check_paths("`yt/not_present yt/**`", Path("/not-used"))

        self.assertEqual(checked, 0)
        self.assertEqual(len(errors), 1)
        self.assertIn("must not contain whitespace", errors[0])

    def test_scoped_paths_use_full_argument_and_canonical_form(self) -> None:
        invalid_commands = [
            "../ya tool ast-index outline yt/server/http_proxy/config.h+typo",
            "../ya tool ast-index outline yt/../ya.make",
            "../ya tool ast-index outline yt//server/http_proxy/config.h",
            "../ya tool ast-index outline yt/yt/server/http_proxy/config.h",
            "../ya tool ast-index outline yt/server/not_present.h",
            "../ya tool ast-index outline 'yt/server/http_proxy/config.h'+typo",
        ]
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            self._touch(repo_root / "yt/yt/server/http_proxy/config.h")
            correct = "`../ya tool ast-index outline yt/server/http_proxy/config.h`"

            self.assertEqual(checker.check_scoped_ast_paths(correct, repo_root), [])
            for command in invalid_commands:
                with self.subTest(command=command):
                    self.assertTrue(checker.check_scoped_ast_paths(f"`{command}`", repo_root))

    def test_scoped_quoted_path_with_spaces_and_placeholder_are_supported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            self._touch(repo_root / "yt/docker/tutorial/About this demo.ipynb")
            markdown = (
                "`../ya tool ast-index outline 'docker/tutorial/About this demo.ipynb'`\n"
                "../ya tool ast-index outline '<path-to-large-file>'"
            )

            errors = checker.check_scoped_ast_paths(markdown, repo_root)

        self.assertEqual(errors, [])

    def test_scoped_extractor_keeps_suffix_until_token_boundary(self) -> None:
        paths, errors = checker.extract_scoped_ast_paths(
            "`../ya tool ast-index outline yt/server/http_proxy/config.h+typo`"
        )

        self.assertEqual(errors, [])
        self.assertEqual(paths, ["yt/server/http_proxy/config.h+typo"])

    def test_scoped_command_without_same_line_argument_is_rejected(self) -> None:
        markdown = "../ya tool ast-index outline\nyt/server/http_proxy/config.h"

        paths, errors = checker.extract_scoped_ast_paths(markdown)

        self.assertEqual(paths, [])
        self.assertEqual(len(errors), 1)
        self.assertIn("no path argument", errors[0])

    def test_negative_review_age_is_rejected(self) -> None:
        with self.assertRaises(argparse.ArgumentTypeError):
            checker.nonnegative_int("-1")

    def test_cli_date_is_strict_yyyy_mm_dd(self) -> None:
        for value in ("20260901", "2026-W36-2", "2026-9-1", "2026-02-30"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                checker.iso_date(value)

    def test_reviewed_on_is_strict_even_when_age_limit_is_disabled(self) -> None:
        today = dt.date(2026, 9, 1)

        for value in (None, "bad", "20260901", "2026-W36-2", "2026-09-02"):
            with self.subTest(value=value):
                inventory = {} if value is None else {"reviewed_on": value}
                errors, age = checker.check_review_age(inventory, 0, today=today)
                self.assertTrue(errors)
                self.assertIsNone(age)

    def test_review_age_boundary(self) -> None:
        today = dt.date(2026, 9, 1)

        errors, age = checker.check_review_age({"reviewed_on": "2026-06-03"}, 90, today=today)
        self.assertEqual(errors, [])
        self.assertEqual(age, 90)

        errors, age = checker.check_review_age({"reviewed_on": "2026-06-02"}, 90, today=today)
        self.assertEqual(len(errors), 1)
        self.assertEqual(age, 91)

    def test_inventory_schema_version_requires_integer_one(self) -> None:
        for schema_version in (True, 1.0, "1", 2):
            with self.subTest(schema_version=schema_version), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "inventory.json"
                path.write_text(
                    json.dumps(
                        {
                            "schema_version": schema_version,
                            "top_level_directories": [],
                        }
                    ),
                    encoding="utf-8",
                )
                with self.assertRaises(ValueError):
                    checker.load_inventory(path)

    def test_inventory_shape_and_directory_order_errors_are_controlled(self) -> None:
        invalid_values = [
            [],
            {"schema_version": 1, "top_level_directories": ["a", 1]},
            {"schema_version": 1, "top_level_directories": [""]},
            {"schema_version": 1, "top_level_directories": ["b", "a"]},
            {"schema_version": 1, "top_level_directories": ["a", "a"]},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inventory.json"
            for value in invalid_values:
                with self.subTest(value=value):
                    path.write_text(json.dumps(value), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        checker.load_inventory(path)

    def test_inventory_comparison_reports_added_and_removed(self) -> None:
        errors = checker.compare_inventory({"top_level_directories": ["old", "same"]}, ["new", "same"])

        self.assertEqual(len(errors), 2)
        self.assertIn("new", errors[0])
        self.assertIn("old", errors[1])

    @mock.patch("check_codebase_map.subprocess.run")
    def test_tracked_directories_come_from_arc_head(self, run: mock.Mock) -> None:
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="yt/b\nyt/a\n", stderr="")

        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            (repo_root / "yt/generated_tmp").mkdir(parents=True)
            (repo_root / "target").mkdir()
            (repo_root / "yt/symlink_dir").symlink_to(repo_root / "target", target_is_directory=True)

            directories = checker.tracked_top_level_directories(repo_root)

        self.assertEqual(directories, ["a", "b"])

    def test_arc_ls_tree_errors_are_controlled(self) -> None:
        outcomes = [
            FileNotFoundError(),
            subprocess.TimeoutExpired(cmd="arc", timeout=60),
            subprocess.CalledProcessError(1, ["arc"], stderr="boom"),
            subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout="yt/a/b\n", stderr=""),
        ]
        for outcome in outcomes:
            with self.subTest(outcome=type(outcome).__name__), mock.patch("check_codebase_map.subprocess.run") as run:
                if isinstance(outcome, BaseException):
                    run.side_effect = outcome
                else:
                    run.return_value = outcome
                with self.assertRaises(ValueError):
                    checker.tracked_top_level_directories(Path("/repo"))

    def test_refresh_refuses_invalid_map_and_preserves_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            map_path, inventory_path = self._write_fixture(repo_root, "Broken: `yt/not_present`", ["a"], "2026-08-01")
            original = inventory_path.read_bytes()
            args = self._args(repo_root, map_path, inventory_path, refresh=True)

            result = self._run_main_with_directories(args, ["a"])

            self.assertEqual(result, 1)
            self.assertEqual(inventory_path.read_bytes(), original)

    def test_attestation_refuses_suffix_bypass_and_preserves_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            self._touch(repo_root / "yt/prefix")
            map_path, inventory_path = self._write_fixture(repo_root, "Broken: `yt/prefix+typo`", ["a"], "2026-08-01")
            original = inventory_path.read_bytes()
            args = self._args(
                repo_root,
                map_path,
                inventory_path,
                reviewed_on=dt.date(2026, 9, 1),
            )

            result = self._run_main_with_directories(args, ["a"])

            self.assertEqual(result, 1)
            self.assertEqual(inventory_path.read_bytes(), original)

    def test_attestation_refuses_arbitrary_backtick_span_and_preserves_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            map_path, inventory_path = self._write_fixture(
                repo_root, "Broken: ```yt/not_present```", ["a"], "2026-08-01"
            )
            original = inventory_path.read_bytes()
            args = self._args(
                repo_root,
                map_path,
                inventory_path,
                reviewed_on=dt.date(2026, 9, 1),
            )

            result = self._run_main_with_directories(args, ["a"])

            self.assertEqual(result, 1)
            self.assertEqual(inventory_path.read_bytes(), original)

    def test_attestation_refuses_multiline_span_and_preserves_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            map_path, inventory_path = self._write_fixture(
                repo_root,
                "`\nyt/not_present\n`",
                ["a"],
                "2026-08-01",
            )
            original = inventory_path.read_bytes()
            args = self._args(
                repo_root,
                map_path,
                inventory_path,
                reviewed_on=dt.date(2026, 9, 1),
            )

            result = self._run_main_with_directories(args, ["a"])

            self.assertEqual(result, 1)
            self.assertEqual(inventory_path.read_bytes(), original)

    def test_attestation_refuses_pattern_hiding_neighbor_and_preserves_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            map_path, inventory_path = self._write_fixture(
                repo_root,
                "Broken: `yt/not_present yt/**`",
                ["a"],
                "2026-08-01",
            )
            original = inventory_path.read_bytes()
            args = self._args(
                repo_root,
                map_path,
                inventory_path,
                reviewed_on=dt.date(2026, 9, 1),
            )

            result = self._run_main_with_directories(args, ["a"])

            self.assertEqual(result, 1)
            self.assertEqual(inventory_path.read_bytes(), original)

    def test_attestation_refuses_malformed_placeholder_and_preserves_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            map_path, inventory_path = self._write_fixture(repo_root, "Broken: `yt/not_present[`", ["a"], "2026-08-01")
            original = inventory_path.read_bytes()
            args = self._args(
                repo_root,
                map_path,
                inventory_path,
                reviewed_on=dt.date(2026, 9, 1),
            )

            result = self._run_main_with_directories(args, ["a"])

            self.assertEqual(result, 1)
            self.assertEqual(inventory_path.read_bytes(), original)

    def test_refresh_refuses_invalid_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            map_path = repo_root / "map.md"
            inventory_path = repo_root / "inventory.json"
            map_path.write_text("No exact paths.", encoding="utf-8")
            inventory_path.write_text("not json", encoding="utf-8")
            original = inventory_path.read_bytes()
            args = self._args(repo_root, map_path, inventory_path, refresh=True)

            result = self._run_main_with_directories(args, ["a"])

            self.assertEqual(result, 1)
            self.assertEqual(inventory_path.read_bytes(), original)

    def test_refresh_preserves_review_attestation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            map_path, inventory_path = self._write_fixture(repo_root, "No exact paths.", ["old"], "2026-08-01")
            args = self._args(repo_root, map_path, inventory_path, refresh=True)

            result = self._run_main_with_directories(args, ["new"])

            inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
            self.assertEqual(result, 0)
            self.assertEqual(inventory["top_level_directories"], ["new"])
            self.assertEqual(inventory["reviewed_on"], "2026-08-01")

    def test_explicit_attestation_updates_only_reviewed_on(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo_root = Path(directory)
            map_path, inventory_path = self._write_fixture(repo_root, "No exact paths.", ["a"], "2026-01-01")
            args = self._args(
                repo_root,
                map_path,
                inventory_path,
                reviewed_on=dt.date(2026, 9, 1),
            )

            result = self._run_main_with_directories(args, ["a"])

            inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
            self.assertEqual(result, 0)
            self.assertEqual(inventory["top_level_directories"], ["a"])
            self.assertEqual(inventory["reviewed_on"], "2026-09-01")

    @staticmethod
    def _touch(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()

    @staticmethod
    def _write_fixture(repo_root: Path, markdown: str, directories: list[str], reviewed_on: str) -> tuple[Path, Path]:
        map_path = repo_root / "map.md"
        inventory_path = repo_root / "inventory.json"
        map_path.write_text(markdown, encoding="utf-8")
        inventory_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "reviewed_on": reviewed_on,
                    "top_level_directories": directories,
                }
            ),
            encoding="utf-8",
        )
        return map_path, inventory_path

    @staticmethod
    def _args(
        repo_root: Path,
        map_path: Path,
        inventory_path: Path,
        *,
        refresh: bool = False,
        reviewed_on: dt.date | None = None,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            repo_root=repo_root,
            map_path=map_path,
            inventory_path=inventory_path,
            max_review_age_days=0,
            refresh_inventory=refresh,
            reviewed_on=reviewed_on,
        )

    @staticmethod
    def _run_main_with_directories(args: argparse.Namespace, directories: list[str]) -> int:
        with mock.patch.object(checker, "parse_args", return_value=args), mock.patch.object(
            checker, "tracked_top_level_directories", return_value=directories
        ), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return checker.main()


if __name__ == "__main__":
    unittest.main()
