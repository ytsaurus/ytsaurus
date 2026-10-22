import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


source_file = Path(__file__).parents[1] / "__main__.py"
if not source_file.is_file():
    import yatest.common

    source_file = Path(yatest.common.source_path("yt/yt/scripts/generate_documentation_release_notes/__main__.py"))
spec = importlib.util.spec_from_file_location("release_notes", source_file)
notes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(notes)


def response(status=200, body="", data=None):
    result = notes.requests.Response()
    result.status_code = status
    result.url = "https://github.com/example"
    result._content = (json.dumps(data) if data is not None else body).encode()
    return result


def component(artifacts=None):
    return notes.Component("test", "Test", "test/", "Test component", "test.md", artifacts=artifacts)


def release(body="Changes"):
    return {
        "tag_name": "test/0.3.0",
        "created_at": "2026-09-26T23:02:31Z",
        "published_at": "2026-10-02T10:53:10Z",
        "html_url": "https://github.com/ytsaurus/test/releases/tag/test/0.3.0",
        "body": body,
    }


class ReleaseNotesTest(unittest.TestCase):
    def setUp(self):
        notes.repo_name_to_releases.clear()
        notes.package_page_to_html.clear()
        notes.package_to_versions.clear()

    def test_flow_metadata_is_rendered_once_and_created_at_is_preserved(self):
        flow = next(c for c in notes.COMPONENTS if c.component_name == "Flow")
        fields = ["**Release date:** 2026-09-28"]
        for artifact in flow.artifacts:
            fields.append(f"**{artifact.label}:** [0.3.0](https://example.com/{artifact.label.replace(' ', '-')})")
        body = "Introduction.\n\n" + "\n\n".join(fields) + "\n\n## Artifacts\n\nDetailed Java SDK documentation.\n"
        with patch.object(flow, "_get_releases", return_value=[release(body)]), patch.object(flow, "_get_package_page_html") as fetch:
            text = flow._generate_markdown()
        fetch.assert_not_called()
        self.assertIn("**Release date:** 2026-09-26", text)
        self.assertNotIn("2026-09-28", text)
        self.assertNotIn("2026-10-02", text)
        for label in ["Release date", "Release page"] + [a.label for a in flow.artifacts]:
            self.assertEqual(text.count(f"**{label}:**"), 1, label)
        self.assertIn("#### Artifacts\n\nDetailed Java SDK documentation.", text)

    def test_missing_metadata_is_generated_for_any_component(self):
        artifact = notes.Artifact("SDK", "package", version_url_template="https://example.com/{version}")
        c = component([artifact])
        with patch.object(c, "_get_releases", return_value=[release()]):
            self.assertIn("**SDK:** [0.3.0](https://example.com/0.3.0)", c._generate_markdown())

    def test_explicit_package_link_wins_over_discovery(self):
        artifact = notes.DockerArtifact("Docker image", "test", "test", ["{version}"])
        c = component([artifact])
        body = "**Docker image:** [custom](https://example.com/custom)"
        with patch.object(c, "_get_releases", return_value=[release(body)]), patch.object(artifact, "get_info") as discover:
            text = c._generate_markdown()
        discover.assert_not_called()
        self.assertEqual(text.count("https://example.com/custom"), 1)

    def test_metadata_before_heading_without_blank_line_is_rendered_once(self):
        artifact = notes.Artifact("SDK", "package", version_url_template="https://example.com/automatic/{version}")
        c = component([artifact])
        body = "**Release date:** 2026-09-28\n**SDK:** [explicit](https://example.com/sdk)\n## Changes\n\nFixed bug."
        with patch.object(c, "_get_releases", return_value=[release(body)]):
            text = c._generate_markdown()
        self.assertEqual(text.count("**Release date:**"), 1)
        self.assertIn("**Release date:** 2026-09-26", text)
        self.assertNotIn("2026-09-28", text)
        self.assertNotIn("2026-10-02", text)
        self.assertEqual(text.count("**SDK:**"), 1)
        self.assertEqual(text.count("[explicit](https://example.com/sdk)"), 1)
        self.assertNotIn("https://example.com/automatic/", text)
        self.assertIn("#### Changes\n\nFixed bug.", text)

    def test_empty_and_null_descriptions(self):
        for body in [None, ""]:
            with self.subTest(body=body):
                c = component()
                with patch.object(c, "_get_releases", return_value=[release(body)]):
                    self.assertIn("No description", c._generate_markdown())

    def test_identical_fields_are_combined(self):
        metadata, body = notes.prepare_description("**SDK:** same\r\n\r\n**SDK:** same\r\n\r\nText", ["SDK"])
        self.assertEqual(metadata, {"SDK": "same"})
        self.assertEqual(body, "Text")

    def test_conflicting_fields_fail(self):
        with self.assertRaisesRegex(ValueError, "Conflicting values"):
            notes.prepare_description("**SDK:** first\n\n**SDK:** second", ["SDK"])

    def test_metadata_inside_markdown_content_is_preserved(self):
        examples = [
            "```markdown\n**SDK:** example\n# Example heading\n```",
            "~~~~\n**SDK:** example\n```\n~~~~",
            "> **SDK:** example",
            "    **SDK:** example",
            "- **SDK:** example",
            "Paragraph\n**SDK:** example\ncontinued",
            "## Artifacts\n\n**SDK:** detailed explanation",
            "Artifacts\n---------\n\n**SDK:** detailed explanation",
            "**Unknown:** value",
        ]
        for text in examples:
            with self.subTest(text=text):
                metadata, body = notes.prepare_description(text, ["SDK"])
                self.assertEqual(metadata, {})
                self.assertIn("**SDK:**" if "**SDK:**" in text else "**Unknown:**", body)
                if text.startswith(("```", "~~~~")):
                    self.assertEqual(body, text)

    def test_adjacent_fields_inside_paragraphs_are_preserved_as_a_whole(self):
        examples = [
            "Paragraph\n**SDK:** A\n**Release date:** B\n\nEnd",
            "**SDK:** A\n**Release date:** B\ncontinued",
            "Paragraph\n**SDK:** A\n**Release date:** B\ncontinued",
            "**SDK:** A\n**Unknown:** B\n\nEnd",
            "Paragraph\n**SDK:** A\n**SDK:** conflicting",
        ]
        for body in examples:
            with self.subTest(body=body):
                metadata, remaining = notes.prepare_description(body, ["SDK", "Release date"])
                self.assertEqual(metadata, {})
                self.assertEqual(remaining, body)

    def test_html_code_blocks_and_comments_are_preserved(self):
        for opening, closing in [("<pre>", "</pre>"), ("<PRE class=\"example\">", "</PRE>"), ("<!--", "-->")]:
            with self.subTest(opening=opening):
                body = opening + "\n\n**SDK:** [example](https://example.com)\n\n# Heading\n\n" + closing
                metadata, remaining = notes.prepare_description(body, ["SDK"])
                self.assertEqual(metadata, {})
                self.assertEqual(remaining, body)

    def test_raw_html_variants_and_unclosed_blocks_are_preserved(self):
        examples = [
            "<script>\n\n**SDK:** example\n\n# Heading\n</script>",
            "<style>\n\n**SDK:** example\n\n</style>",
            "<textarea>\n\n**SDK:** example\n\n</textarea>",
            "<![CDATA[\n\n**SDK:** example\n\n]]>",
            "<?instruction\n\n**SDK:** example\n\n?>",
            "<!CUSTOM\n\n**SDK:** example\n\n>",
            "<pre>\n\n**SDK:** example\n\n# Unclosed code",
            "<!--\n\n**SDK:** example\n\n# Unclosed comment",
        ]
        for body in examples:
            with self.subTest(body=body):
                metadata, remaining = notes.prepare_description(body, ["SDK"])
                self.assertEqual(metadata, {})
                self.assertEqual(remaining, body)

    def test_real_metadata_after_multiline_html_or_fence_is_extracted(self):
        for block in [
            "<!--\n\n**SDK:** example\n\n# Heading\n-->",
            "<pre>\n```\n\n**SDK:** example\n\n</pre>",
            "```html\n<!--\n\n**SDK:** example\n\n```",
        ]:
            with self.subTest(block=block):
                metadata, remaining = notes.prepare_description(block + "\n\n**SDK:** real\n\nText", ["SDK"])
                self.assertEqual(metadata, {"SDK": "real"})
                self.assertEqual(remaining, block + "\n\nText")

    def test_metadata_after_closed_html_block_is_extracted(self):
        for block in ["<pre>example</pre>", "<!-- comment -->"]:
            with self.subTest(block=block):
                body = block + "\n\n**SDK:** real-link\n\nDescription"
                metadata, remaining = notes.prepare_description(body, ["SDK"])
                self.assertEqual(metadata, {"SDK": "real-link"})
                self.assertEqual(remaining, block + "\n\nDescription")

    def test_html_example_does_not_override_generated_package_link(self):
        artifact = notes.Artifact("SDK", "package", version_url_template="https://real.example/{version}")
        c = component([artifact])
        body = "<pre>\n\n**SDK:** [example](https://example.com)\n\n</pre>"
        with patch.object(c, "_get_releases", return_value=[release(body)]):
            text = c._generate_markdown()
        self.assertIn("**SDK:** [0.3.0](https://real.example/0.3.0)", text)
        self.assertIn(body, text)

    def test_adjacent_fields_and_alternative_bold_colon(self):
        metadata, body = notes.prepare_description("**SDK**: link\n**Release date:** 2026-09-28\n\nText", ["SDK", "Release date"])
        self.assertEqual(metadata, {"SDK": "link", "Release date": "2026-09-28"})
        self.assertEqual(body, "Text")

    def test_api_release_page_wins_and_body_page_is_a_fallback(self):
        c = component()
        r = release("**Release page:** [explicit](https://example.com/release)")
        with patch.object(c, "_get_releases", return_value=[r]):
            self.assertNotIn("https://example.com/release", c._generate_markdown())
            r.pop("html_url")
            self.assertIn("https://example.com/release", c._generate_markdown())

    def test_package_pagination_and_url_parsing(self):
        c = component()
        first = "https://github.com/orgs/ytsaurus/packages/container/test/versions?filters%5Bversion_type%5D=tagged"
        second = "https://github.com/orgs/ytsaurus/packages/container/test/versions?page=2"
        pages = {
            first: '<a href="/orgs/ytsaurus/packages/container/test/10?tag=0.3.0-extra">other</a>'
                   '<a rel="next" href="?page=2">Next</a>',
            second: '<a href="https://github.com/orgs/ytsaurus/packages/container/test/11?other=1&amp;tag=0.3.0">version</a>'
                    '<a href="/orgs/ytsaurus/packages/container/test/12?tag=stable-0.3.0">stable</a>',
        }
        with patch.object(c, "_get_package_page_html", side_effect=pages.__getitem__) as fetch:
            docker = notes.DockerArtifact("Docker image", "unused", "test", ["stable-{version}", "{version}"])
            self.assertEqual(docker.get_info(c, "0.3.0"), (
                "ghcr.io/ytsaurus/test:stable-0.3.0",
                "https://github.com/orgs/ytsaurus/packages/container/test/12?tag=stable-0.3.0",
            ))
            self.assertIsNone(docker.get_info(c, "0.2.0"))
            self.assertEqual(fetch.call_count, 2)

    def test_github_package_version_transform_and_relative_link(self):
        c = component()
        html = '<a href="/ytsaurus/operator/pkgs/container/chart/10?tag=0.3.0">version</a>'
        with patch.object(c, "_get_package_page_html", return_value=html):
            artifact = notes.GithubPackageArtifact("Helm chart", "operator", "chart", version_transform=lambda v: [v, v.removeprefix("v")])
            self.assertEqual(artifact.get_info(c, "v0.3.0"), ("0.3.0", "https://github.com/ytsaurus/operator/pkgs/container/chart/10?tag=0.3.0"))

    def test_package_lookup_ignores_other_packages_and_external_links(self):
        c = component()
        html = '<a href="/orgs/ytsaurus/packages/container/other/10?tag=0.3.0">other</a>'
        html += '<a href="https://example.com/container/test/10?tag=0.3.0">external</a>'
        with patch.object(c, "_get_package_page_html", return_value=html):
            self.assertEqual(c._get_package_versions("container", "test"), {})

    def test_package_pagination_rejects_cycles_and_external_pages(self):
        for href in ["?filters%5Bversion_type%5D=tagged", "https://example.com/container/test/versions"]:
            with self.subTest(href=href):
                c = component()
                with patch.object(c, "_get_package_page_html", return_value=f'<a rel="next" href="{href}">Next</a>'):
                    with self.assertRaises(ValueError):
                        c._get_package_versions("container", "test")
                self.assertNotIn(("container", "test"), notes.package_to_versions)

    def test_package_page_error_does_not_cache_an_empty_result(self):
        c = component()
        with patch.object(notes.requests, "get", return_value=response(404)):
            with self.assertRaises(notes.requests.HTTPError):
                c._get_package_versions("container", "test")
        self.assertEqual(notes.package_page_to_html, {})
        self.assertEqual(notes.package_to_versions, {})

    def test_transient_http_errors_and_timeouts_are_retried(self):
        for failure in [response(429), response(500), notes.requests.Timeout("timeout"), notes.requests.ConnectionError("connection")]:
            with self.subTest(failure=failure):
                with patch.object(notes.requests, "get", side_effect=[failure, response()]) as fetch, patch.object(notes.time, "sleep"):
                    self.assertEqual(notes.get_url("https://example.com").status_code, 200)
                    self.assertEqual(fetch.call_count, 2)
                    self.assertEqual(fetch.call_args.kwargs["timeout"], (10, 30))

    def test_permanent_http_errors_fail_without_retry(self):
        for status in [403, 404]:
            with self.subTest(status=status):
                with patch.object(notes.requests, "get", return_value=response(status)) as fetch:
                    with self.assertRaises(notes.requests.HTTPError):
                        notes.get_url("https://example.com")
                    self.assertEqual(fetch.call_count, 1)

    def test_retries_are_bounded(self):
        for failure in [response(500), notes.requests.Timeout("timeout")]:
            with self.subTest(failure=failure):
                kwargs = {"side_effect": failure} if isinstance(failure, Exception) else {"return_value": failure}
                with patch.object(notes.requests, "get", **kwargs) as fetch, patch.object(notes.time, "sleep"):
                    with self.assertRaises(notes.requests.RequestException):
                        notes.get_url("https://example.com")
                    self.assertEqual(fetch.call_count, 3)

    def test_partial_release_list_is_not_cached_or_replaced_with_fallback(self):
        c = component()
        c.fallback_releases = [release("fallback")]
        with patch.object(c, "_get_releases_page", side_effect=[[release()], notes.requests.HTTPError("failed")]):
            with self.assertRaises(notes.requests.HTTPError):
                c._generate_markdown()
        self.assertNotIn(c.repo_name, notes.repo_name_to_releases)

    def test_successful_empty_list_uses_fallback(self):
        c = component()
        c.fallback_releases = [release("fallback")]
        with patch.object(c, "_get_releases_page", return_value=[]):
            self.assertIn("fallback", c._generate_markdown())

    def test_unexpected_api_payload_fails(self):
        with patch.object(notes.requests, "get", return_value=response(data={"message": "unexpected"})):
            with self.assertRaises(ValueError):
                component()._get_releases_page()

    def test_generation_failure_exits_nonzero_without_writing_any_component(self):
        good, bad = component(), component()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / good.filename
            path.write_text("existing content")
            with (
                patch.object(notes, "COMPONENTS", [good, bad]),
                patch.object(good, "_generate_markdown", return_value="new content"),
                patch.object(bad, "_generate_markdown", side_effect=ValueError("failed")),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                with self.assertRaises(SystemExit) as error:
                    notes.main(["-o", directory])
            self.assertEqual(error.exception.code, 1)
            self.assertEqual(path.read_text(), "existing content")

    def test_successful_generation_writes_identical_results_to_all_outputs(self):
        c = component()
        with tempfile.TemporaryDirectory() as directory:
            paths = [str(Path(directory) / name) for name in ["first", "second"]]
            with patch.object(notes, "COMPONENTS", [c]), patch.object(c, "_get_releases", return_value=[release()]):
                notes.main(["-o", paths[0], "-o", paths[1]])
                first = (Path(paths[0]) / c.filename).read_bytes()
                notes.main(["-o", paths[0], "-o", paths[1]])
            self.assertEqual(first, (Path(paths[0]) / c.filename).read_bytes())
            self.assertEqual(first, (Path(paths[1]) / c.filename).read_bytes())


if __name__ == "__main__":
    unittest.main()
