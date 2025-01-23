import yt.wrapper as yt
import yt.yson as yson

import boto3
import jinja2

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from string import ascii_lowercase
from typing import Any
import argparse
import json
import os
import random
import shlex
import shutil
import subprocess


class HTMLIndexer:
    INDEX_HTML_TEMPLATE = """<!DOCTYPE html>
    <html lang="en">
    <head>
        <title>Index of {{ path }}</title>
        <style>
            body {
                font-family: monospace;
                font-size: 0.95em;
                background-color: white;
            }
            a {
                text-decoration: none;
            }
            a:hover {
                text-decoration: underline;
            }
            td {
                padding: 0px 5px 0px 5px;
                border-right: 1px dotted #aaa;
                white-space: nowrap;
            }
            .title {
                white-space: nowrap;
            }
            .head-link {
                color: #aaa;
            }
            .file-row {
                float: left;
                width: 655px;
            }
        </style>
    </head>
    <body>
        <h3 class="title">
            Index of {{ path }}:
        </h3>
        <table cellspacing="1" cellpadding="0">
            <tr>
                <td><a title=".." href="../index.html"><b>..</b></a></td>
                <td>&mdash;</td>
                <td>&mdash;</td>
            </tr>
            {% set bg_color_cycle = cycler('#EEC', '#FFF') %}
            {% for folder in folders %}
            <tr style="background-color: {{ bg_color_cycle.next() }}">
                    <td><a title="{{ folder.name }}" href="{{ folder.href }}">{{ folder.name }}/</a></td>
                    <td>&mdash;</td>
                    <td>&mdash;</td>
                </tr>
            {% endfor %}
            {% for file in files %}
            <tr style="background-color: {{ bg_color_cycle.next() }}">
                <td><a title="{{ file.name }}" href="{{ file.href }}">{{ file.name }}</a></td>
                <td>{{ file.size }}</td>
                <td>{{ file.modification_time }}</td>
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>"""

    @classmethod
    def generate_index_html(cls, url_prefix: str, path: Path):
        path = Path(path)
        files: list[dict[str, str]] = []
        for file in path.iterdir():
            if file.is_file():
                if file.name == "index.html":
                    continue
                file_modification_time = datetime.fromtimestamp(Path(file).stat().st_mtime).strftime("%Y.%m.%d %H:%M:%S")
                files.append(
                    {
                        "name": file.name,
                        "href": file.name,
                        "size": f"{file.stat().st_size / 1024:.2f}KiB",
                        "modification_time": file_modification_time,
                    }
                )
        files.sort(key=lambda x: x["name"])

        folders: list[dict[str, str]] = []
        for folder in path.iterdir():
            if folder.is_dir():
                cls.generate_index_html(url_prefix, folder)
                folders.append({"name": folder.name, "href": folder.name + "/index.html"})
        folders.sort(key=lambda x: x["name"])

        template = jinja2.Template(cls.INDEX_HTML_TEMPLATE)
        html_content = template.render(path=path, files=files, folders=folders)

        with open(path / "index.html", "w") as f:
            f.write(html_content)


@dataclass
class BuildReportResultDataclass:
    chunk_hid: int | None = None
    duration: float | None = None
    hid: int | None = None
    id: str | None = None
    links: dict[str, list[str]] | None = None
    metrics: dict[str, float] | None = None
    name: str | None = None
    path: str | None = None
    size: str | None = None
    status: str | None = None
    subtest_name: str | None = None
    suite_hid: int | None = None
    tags: list[str] | None = None
    type: str | None = None
    uid: str | None = None
    owners: dict[str, list[str]] | None = None
    toolchain: str | None = None
    requirements: dict[str, int] | None = None
    rich_snippet: str | None = None
    suite: bool | None = None
    suite_status: str | None = None
    chunk: bool | None = None
    error_type: str | None = None


class BuildReportParser:
    def __init__(self, build_report_path: str, output_path: str):
        self.build_report_path: str = build_report_path
        self.output_path: str = output_path

    def dump_logs(self):
        if not Path(self.build_report_path).exists():
            return
        with open(self.build_report_path, "rb") as build_report:
            build_report_data: dict[str, Any] = json.load(build_report)
        for result in build_report_data["results"]:
            if "rich-snippet" in result:
                result["rich_snippet"] = result["rich-snippet"]
                del result["rich-snippet"]
            result = BuildReportResultDataclass(**result)
            if result.type != "test":
                continue
            if not isinstance(result.links, dict):
                continue
            logs = result.links.get("log", [])
            for log in logs:
                results_index = log.index("test-results")
                destination = log[results_index:]
                Path(self.output_path, destination).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(log, Path(self.output_path, destination))


class TestRunner:
    def __init__(
        self,
        test_path: str,
        output_path: str,
        num_threads: int,
        yt_root: str,
        ya_test_flags: str,
    ):
        self.test_path: str = str(Path(yt_root, test_path))
        self.output_path: str = output_path
        self.num_threads: int = num_threads
        self.yt_root: str = yt_root
        self.ya_test_flags: list[str] = shlex.split(ya_test_flags)
        self.return_code: int = 1
        self.tmp_dir: str = str(Path("tmp", "".join(random.choices(ascii_lowercase, k=10))).absolute())

    def run_test(self):
        command = [
            str(Path(self.yt_root, "ya").absolute()),
            "make",
            *self.ya_test_flags,
            f"-j{self.num_threads}",
            f"--output={self.tmp_dir}",
            "--build-results-report=build_results_report.json",
            self.test_path,
        ]
        print("Running:\n", " ".join(command))
        self.return_code = subprocess.call(command)

    def parse(self):
        parser = BuildReportParser(
            str(Path(self.tmp_dir, "build_results_report.json").absolute()),
            self.output_path,
        )
        parser.dump_logs()


class S3Uploader:
    def __init__(self, bucket_name: str, upload_path: str, endpoint_url: str):
        self.bucket_name: str = bucket_name
        self.upload_path: str = upload_path
        self.endpoint_url: str = endpoint_url

    def make_index(self):
        HTMLIndexer.generate_index_html(
            self.endpoint_url + "/" + self.bucket_name,
            Path(self.upload_path),
        )

    def upload_artifacts(self):
        session = boto3.session.Session()
        s3 = session.client(
            service_name="s3",
            endpoint_url=self.endpoint_url,
        )
        for root, _, files in os.walk(self.upload_path):
            for file in files:
                # print(file)
                if file.endswith("html"):
                    extra_args = {"ContentType": "text/html"}
                else:
                    extra_args = {"ContentType": "text/plain"}
                s3.upload_file(
                    str(Path(root, file)),
                    self.bucket_name,
                    str(Path(root, file)),
                    ExtraArgs=extra_args,
                )


class DumpToYt:
    def __init__(
        self,
        dump_script_path: str,
        input_ci_context: str,
        s3_bucket_name: str | None,
        storage_cluster: str,
        storage_token: str,
        storage_table_builds: str,
        storage_table_tests: str,
        artifacts_endpoint_url: str,
    ):
        self.dump_script_path: str = dump_script_path
        self.input_ci_context: str = input_ci_context
        self.s3_bucket_name: str | None = s3_bucket_name
        self.storage_cluster: str = storage_cluster
        self.storage_token: str = storage_token
        self.python_bin_path: str = shutil.which("python3")
        self.storage_table_builds: str = storage_table_builds
        self.storage_table_tests: str = storage_table_tests
        self.artifacts_endpoint_url: str = artifacts_endpoint_url

    def dump(self, tests: list[TestRunner], no_upload_result: bool, dump_build_id: str | None):
        input_artifacts: dict[str, str] = {}
        input_report: list[str] = []

        for test in tests:
            if self.s3_bucket_name:
                input_artifacts[test.output_path] = (
                    f"{self.artifacts_endpoint_url}/{self.s3_bucket_name}/{test.output_path}/index.html"
                )
            input_report.append("--input-report=" + str(Path(test.tmp_dir, "build_results_report.json")))

        command: list[str] = [
            self.python_bin_path,
            self.dump_script_path,
            "--input-ci-context",
            self.input_ci_context,
            *input_report,
            "--input-artifacts",
            str(json.dumps(input_artifacts)),
            "--output-data",
            "tmp/output_data.json",
        ]

        if no_upload_result:
            command.append("--no-upload-result")

        if dump_build_id:
            command = command + ["--build-id", dump_build_id]

        print("Running:\n", " ".join(command))
        subprocess.call(
            command,
            env={
                "STORAGE_CLUSTER": self.storage_cluster,
                "STORAGE_YT_TOKEN": self.storage_token,
                "STORAGE_TABLE_BUILDS": self.storage_table_builds,
                "STORAGE_TABLE_TESTS": self.storage_table_tests,
            },
        )

        for test in tests:
            command: list[str] = [
                self.python_bin_path,
                self.dump_script_path,
                "--no-upload-result",
                "--input-ci-context",
                self.input_ci_context,
                "--input-report",
                str(Path(test.tmp_dir, "build_results_report.json")),
                "--input-artifacts",
                str(json.dumps({test.output_path: input_artifacts[test.output_path]})) if self.s3_bucket_name else "{}",
                "--output-data",
                test.tmp_dir + "/output_data.json",
            ]
            subprocess.call(
                command,
                env={
                    "STORAGE_CLUSTER": self.storage_cluster,
                    "STORAGE_YT_TOKEN": self.storage_token,
                },
            )


class GithubCommentGenerator:
    @classmethod
    def _generate_table_from_report(cls, report: dict[str, Any], title: str) -> str:
        lines: list[str] = []
        lines.append(f"#### {title}")
        lines.append("| Total | Failed | Ok | Skipped | Not launched |")
        lines.append("|-------|--------|----|---------|--------------|")
        for report_type, title in zip(("total_tests", ), ("Tests", )):
            total = report[report_type]["total"]
            failed = report[report_type]["failed"]
            ok = report[report_type]["ok"]
            skipped = report[report_type]["skipped"]
            not_launched = report[report_type]["not_launched"]
            lines.append(f"| {total} | {failed} | {ok} | {skipped} | {not_launched} |")
        return "\\n".join(lines)

    @classmethod
    def generate_comment(cls, test_info_path: str, tests: list[TestRunner] | None = None) -> str:
        lines: list[str] = []
        if not Path(test_info_path).exists():
            return "Something went wrong."
        with open(test_info_path, "rb") as file:
            data = json.load(file)
        lines.append(cls._generate_table_from_report(data, "Total"))
        if tests:
            for test in tests:
                if not Path(test.tmp_dir + "/output_data.json").exists():
                    lines.append(f"#### {test.output_path} failed to load. (returncode {test.return_code})")
                    continue
                with open(test.tmp_dir + "/output_data.json", "rb") as file:
                    test_data = json.load(file)
                if test_data["artifacts"]:
                    artifact = test_data["artifacts"][test.output_path]
                    title = f"[{test.output_path}]({artifact}) (returncode {test.return_code})"
                else:
                    title = test.output_path
                lines.append(cls._generate_table_from_report(test_data, title))
        lines.append(f"### [Failed suites](http://ci-viewer.dev.ytsaurus.tech/build/{data['uid']})")
        return "\\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        prog="Run and dump tests",
    )

    parser.add_argument(
        "--yt-root",
        help="Path to the yt root directory",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--ya-test-flags",
        help="Flags to pass to `ya make`",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--test-to-run",
        help="Test configuration in the format 'test_path:output_path:num_threads', can be specified multiple times."
        "Example: `yt/yt/tests/integration/size_s:dump/size_s:16`."
        "test_path is relative to the yt root directory, output_path is relative to the execution directory.",
        type=str,
        default=[],
        action="append",
        required=True,
    )
    parser.add_argument(
        "--s3-upload-artifacts",
        help="If set, the artifacts will be uploaded to the S3 bucket. Every test's outputs will be uploaded to <bucket_name>/<output_path>",
        type=str,
        required=False,
        default="",
    )
    parser.add_argument(
        "--s3-endpoint-url",
        help="Endpoint URL of the S3 bucket",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--s3-bucket-name",
        help="Name of the S3 bucket to upload the artifacts to",
        type=str,
        required=False,
        default="",
    )
    parser.add_argument(
        "--dump-storage-script",
        help="If set, the tests will be dumped to the storage cluster using provided script",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--dump-no-upload-result",
        help="If set, the tests will not be uploaded to the storage cluster",
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--dump-storage-cluster",
        help="Storage cluster to dump the tests to",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--dump-storage-token",
        help="Token to access the storage cluster",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--dump-storage-table-builds",
        help="YPath to the table to dump the builds to",
        type=str,
        default="//home/ci/ci_builds",
    )
    parser.add_argument(
        "--dump-storage-table-tests",
        help="YPath to the table to dump the tests to",
        type=str,
        default="//home/ci/ci_tests",
    )
    parser.add_argument(
        "--dump-storage-build-id",
        help="If not set, id will be generated randomly as uuid",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--dump-storage-ci-context-path",
        help="Path to the CI context file",
        type=str,
        required=False,
    )

    args = parser.parse_args()

    test_runners: list[TestRunner] = []
    for test_config in args.test_to_run:
        assert (
            len(test_config.split(":")) == 3
        ), "Test configuration must be in the format 'test_path:output_path:num_threads'"
        test_path, output_path, num_threads = test_config.split(":")
        assert test_path != "", "Test path must be provided"
        assert output_path != "", "Output path must be provided"
        assert num_threads.isdigit(), "Number of threads must be an integer"
        test_runners.append(
            TestRunner(
                test_path,
                output_path,
                int(num_threads),
                args.yt_root,
                args.ya_test_flags,
            )
        )

    for test in test_runners:
        test.run_test()

    for test in test_runners:
        test.parse()

    if args.s3_upload_artifacts:
        assert args.s3_bucket_name, "S3 bucket name must be provided"
        uploader = S3Uploader(args.s3_bucket_name, args.s3_upload_artifacts, args.s3_endpoint_url)
        uploader.make_index()
        uploader.upload_artifacts()

    if args.dump_storage_script:
        assert args.dump_storage_cluster, "Storage cluster must be provided"
        assert args.dump_storage_token, "Storage token must be provided"
        assert args.dump_storage_ci_context_path, "CI context must be provided"
        dumper = DumpToYt(
            args.dump_storage_script,
            args.dump_storage_ci_context_path,
            args.s3_bucket_name,
            args.dump_storage_cluster,
            args.dump_storage_token,
            args.dump_storage_table_builds,
            args.dump_storage_table_tests,
            args.s3_endpoint_url,
        )
        dumper.dump(test_runners, args.dump_no_upload_result, args.dump_storage_build_id)

    comment = GithubCommentGenerator.generate_comment("tmp/output_data.json", test_runners)
    with open("test_results.txt", "w") as file:
        file.write(comment)


if __name__ == "__main__":
    main()
