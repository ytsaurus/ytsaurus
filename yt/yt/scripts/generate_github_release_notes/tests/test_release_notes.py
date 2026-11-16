import argparse
import importlib.util
import subprocess
import unittest
from unittest.mock import patch

import yatest.common


def load_source(name, path):
    spec = importlib.util.spec_from_file_location(name, yatest.common.source_path(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


generator = load_source("generate_github_release_notes", "yt/yt/scripts/generate_github_release_notes/__main__.py")

SCHEDULER_BLOCK = "* Changelog entry\nType: fix\nComponent: scheduler\n\nScheduler no longer loses pool weights.\n"
FLOW_BLOCK = "* Changelog entry\nType: feature\nComponent: flow\n\nThe Java SDK reports per-partition watermarks.\n"
SECOND_FLOW_BLOCK = "- Changelog entry\nType: fix\nComponent: flow\n\nPython companion restarts cleanly.\n"


def git_log_output(commits):
    return "\n".join(f"{commit_hash}\n{body}\n{generator.DELIMITER}" for commit_hash, body in commits)


def exported_body(subject, *blocks):
    return f"{subject}\n\nInternal context.\n\n" + "\n".join(blocks) + "commit_hash:0123456789abcdef\n"


def fake_git(commits, returncode=0):
    def run(args, check=False, **kwargs):
        if returncode and check:
            raise subprocess.CalledProcessError(returncode, args)
        return subprocess.CompletedProcess(args, returncode, stdout=git_log_output(commits), stderr="")

    return run


def generator_arguments(**overrides):
    arguments = dict(
        since_commit="flow/0.3.0",
        until_commit="flow/0.4.0",
        repo="ytsaurus",
        components=None,
        ignore_components=None,
        ignore_unparsed=False,
        print_removed=False,
        since_commit_prev_release=None,
        until_commit_prev_release=None,
    )
    arguments.update(overrides)
    return argparse.Namespace(**arguments)


def generate(commits, **overrides):
    with patch.object(generator.subprocess, "run", fake_git(commits)):
        return generator.generate_release_notes(generator_arguments(**overrides)).get_formatted()


def link(commit_hash):
    return f"[{commit_hash[:7]}](https://github.com/ytsaurus/ytsaurus/commit/{commit_hash})"


class SharedGeneratorTest(unittest.TestCase):
    mixed_hash = "a" * 40

    def test_mixed_blocks_keep_every_component(self):
        for blocks in [(SCHEDULER_BLOCK, FLOW_BLOCK), (FLOW_BLOCK, SCHEDULER_BLOCK)]:
            text = generate([(self.mixed_hash, exported_body("YT-1: Mixed change", *blocks))])
            self.assertIn(
                f"### scheduler\nFixes:\n- Scheduler no longer loses pool weights, {link(self.mixed_hash)}.", text
            )
            self.assertIn(
                f"### flow\nFeatures:\n- The Java SDK reports per-partition watermarks, {link(self.mixed_hash)}.", text
            )

    def test_mixed_blocks_filtered_to_flow(self):
        for blocks in [(SCHEDULER_BLOCK, FLOW_BLOCK), (FLOW_BLOCK, SCHEDULER_BLOCK)]:
            text = generate([(self.mixed_hash, exported_body("YT-1: Mixed change", *blocks))], components=["flow"])
            self.assertEqual(
                f"### flow\nFeatures:\n- The Java SDK reports per-partition watermarks, {link(self.mixed_hash)}.\n",
                text,
            )

    def test_two_flow_blocks_are_both_rendered_once(self):
        text = generate(
            [(self.mixed_hash, exported_body("YTFLOW: Two changes", FLOW_BLOCK, SECOND_FLOW_BLOCK))],
            components=["flow"],
        )
        self.assertEqual(
            "### flow\n"
            "Features:\n"
            f"- The Java SDK reports per-partition watermarks, {link(self.mixed_hash)}.\n"
            "Fixes:\n"
            f"- Python companion restarts cleanly, {link(self.mixed_hash)}.\n",
            text,
        )

    def test_single_block_rendering_is_unchanged(self):
        commit_hash = "b" * 40
        body = (
            "YT-29809: Add operation-level job interruption switch\n\n"
            "Add `enable_job_interrupts` to the common operation spec\n\n"
            "* Changelog entry\n"
            "  Type: feature\n"
            "  Component: map-reduce\n\n"
            "  Add `enable_job_interrupts` operation spec option.\n"
            "commit_hash:882bb2a5496f596641b33f12a1042e7f561843d8\n"
        )
        self.assertEqual(
            f"### map-reduce\nFeatures:\n- Add `enable_job_interrupts` operation spec option, {link(commit_hash)}.\n",
            generate([(commit_hash, body)]),
        )

    def test_exporter_trailers_are_not_part_of_the_message(self):
        commit_hash = "c" * 40
        body = (
            "YTFLOW: Change\n\n" + FLOW_BLOCK + "\n---\nPull Request resolved: https://example.com/1\ncommit_hash:abc\n"
        )
        self.assertEqual(
            f"### flow\nFeatures:\n- The Java SDK reports per-partition watermarks, {link(commit_hash)}.\n",
            generate([(commit_hash, body)], components=["flow"]),
        )

    def test_empty_description_is_omitted(self):
        self.assertEqual("", generate([("d" * 40, "YTFLOW: Internal refactoring\ncommit_hash:abc\n")]))

    def test_malformed_block_is_unparsed_and_can_be_ignored(self):
        commit_hash = "e" * 40
        body = exported_body("YTFLOW: Broken", "* Changelog entry\nComponent: flow\n", FLOW_BLOCK)
        self.assertIn("### UNPARSED", generate([(commit_hash, body)]))
        self.assertEqual(
            f"### flow\nFeatures:\n- The Java SDK reports per-partition watermarks, {link(commit_hash)}.\n",
            generate([(commit_hash, body)], components=["flow"], ignore_unparsed=True),
        )

    def test_unknown_type_is_rendered_as_other(self):
        commit_hash = "f" * 40
        body = exported_body(
            "YTFLOW: Tune", "* Changelog entry\nType: improvement\nComponent: flow\n\nFaster startup.\n"
        )
        self.assertEqual(f"### flow\nOther:\n- Faster startup, {link(commit_hash)}.\n", generate([(commit_hash, body)]))

    def test_kafka_tls_commit_renders_both_components(self):
        commit_hash = "1356330097cfd0351758a616afebb0db4a783c1c"
        body = (
            "[kafka] feature YT-28439: add tls for kafka-proxy\n\n"
            "* Changelog entry\n"
            "Type: feature\n"
            "Component: kafka-proxy\n\n"
            "Added TLS support for Kafka proxy\n\n"
            "Enable via config:\n"
            "- `server.credentials.cert_chain.file_name` – certificate chain path\n"
            "- `server.credentials.private_key.file_name` – private key path\n"
            "- `server.credentials.update_period` (optional) – auto-reload interval for cert/key\n\n"
            "Compat note: Listener settings moved under `server`: `port` (top-level `port` still works and is copied "
            "to `server/port`), `bind_retry_count`, `bind_retry_backoff`, `max_simultaneous_connections`, "
            "`max_backlog_size`, `read_idle_timeout`, `write_idle_timeout`.\n\n"
            "* Changelog entry\n"
            "Type: feature\n"
            "Component: proxy\n\n"
            "COMPAT: Previously UpdatePeriod = 0 disabled certificate updates. "
            "Now a missing value disables updates, and 0 means continuous updates.\n"
            "commit_hash:189a4c274803a5dee14cff91cbd73fa586be6115\n"
        )
        text = generate([(commit_hash, body)])
        kafka_section, proxy_section = text.split("### proxy\n")
        self.assertTrue(kafka_section.startswith("### kafka-proxy\nFeatures:\n- Added TLS support for Kafka proxy\n"))
        self.assertIn("`write_idle_timeout`, " + link(commit_hash) + ".\n", kafka_section)
        self.assertNotIn("UpdatePeriod", kafka_section)
        self.assertEqual(
            "Features:\n"
            "- COMPAT: Previously UpdatePeriod = 0 disabled certificate updates. "
            f"Now a missing value disables updates, and 0 means continuous updates, {link(commit_hash)}.\n",
            proxy_section,
        )
        self.assertEqual(text, generate([(commit_hash, body)], components=["kafka-proxy", "proxy"]))

    def test_failed_git_log_raises(self):
        with patch.object(generator.subprocess, "run", fake_git([], returncode=128)):
            with self.assertRaises(subprocess.CalledProcessError):
                generator.generate_release_notes(generator_arguments())
