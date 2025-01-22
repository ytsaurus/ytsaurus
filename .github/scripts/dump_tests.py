import argparse
import os
import simplejson as json
import typing
import uuid
import yt.wrapper as yt

from dataclasses import dataclass, asdict


STORAGE_CLUSTER = os.environ["STORAGE_CLUSTER"]
STORAGE_TABLE_BUILDS = os.environ.get("STORAGE_TABLE_BUILDS", "//home/ci/ci_builds")
STORAGE_TABLE_TESTS = os.environ.get("STORAGE_TABLE_TESTS", "//home/ci/ci_tests")
STORAGE_TTL_BUILDS = 86400000 * 30  # 30 days
STORAGE_TTL_TESTS = 86400000 * 7  # 7 days
YT_TOKEN = os.environ["STORAGE_YT_TOKEN"]


def process_data(data):

    @dataclass
    class Total:
        ok: int = None
        failed: int = None
        skipped: int = None
        not_launched: int = None
        total: int = None
        suite_faield_w_no_tests: int = None

        def __add__(self, other):
            self.ok += other.ok
            self.failed += other.failed
            self.skipped += other.skipped
            self.not_launched += other.not_launched
            self.total += other.total
            self.suite_faield_w_no_tests += other.suite_faield_w_no_tests
            return self

    class Base:
        index: dict = {}

        def __init__(self):
            self.id: str = None
            self.hid: str = None
            self.type: str = None
            self.status: str = None
            self.rich_snippet: str = None
            self.name: str = None
            self.path: str = None
            self.size: str = None
            self.duration: float = None

        @classmethod
        def from_rec(cls, rec):
            if rec.get("type") != "test" or rec.get("name") in [""]:
                return None

            obj = cls()
            for f in ["name", "path", "id", "hid", "type", "status", "duration", "size"]:
                setattr(obj, f, rec.get(f, None))

            for f in ["rich-snippet"]:
                setattr(obj, f.replace("-", "_"), rec.get(f, None))

            return obj

        @classmethod
        def reset_index(cls):
            setattr(cls, "index", {})

        @classmethod
        def add_to_index(cls, obj):
            cls.index[obj.hid] = obj

        @classmethod
        def get_from_index(cls, hid):
            return cls.index[hid]

        @classmethod
        def get_failed_from_index(cls):
            return filter(lambda r: r.is_failed(), cls.index.values())

        @classmethod
        def get_all_from_index(cls):
            return cls.index.values()

        def is_failed(self):
            return self.status == "FAILED"

        def is_ok(self):
            return self.status == "OK"

        def is_skipped(self):
            return self.status == "SKIPPED"

        def is_not_launched(self):
            return self.status == "NOT_LAUNCHED"

    class Suite(Base):
        def __init__(self):
            super().__init__()
            self.sute: bool
            self.suite_status: str
            self._chunks = []
            self._tests = []

        @classmethod
        def from_rec(cls, rec):
            obj = super().from_rec(rec)

            if not obj or "suite" not in rec:
                return None

            for f in ["suite", "suite_status"]:
                setattr(obj, f, rec.get(f, None))

            cls.add_to_index(obj)

            return obj

        @classmethod
        def bind_chunk(cls, obj):
            cls.get_from_index(obj.suite_hid)._chunks.append(obj)

        @classmethod
        def bind_test(cls, obj):
            cls.get_from_index(obj.suite_hid)._tests.append(obj)

        def get_failed_chunks(self):
            return filter(lambda r: r.is_failed(), self._chunks)

        def get_failed_tests(self):
            return filter(lambda r: r.is_failed(), self._tests)

        def get_all_tests(self):
            return self._tests

        def get_html(self):
            return f"""

            [[suite]]{self.path}[[rst]] <{self.name}>
            {self.rich_snippet}
            """

        def as_dict(self):
            return dict((k, v) for k, v in self.__dict__.items() if not k.startswith("_"))

    class Chunk(Base):
        def __init__(self):
            super().__init__()
            self.chunk: bool
            self.suite_hid: str
            self.subtest_name: str
            self._tests = []

        @classmethod
        def from_rec(cls, rec):
            obj = super().from_rec(rec)

            if not obj or "chunk" not in rec:
                return None

            for f in ["chunk", "suite_hid", "subtest_name"]:
                setattr(obj, f, rec.get(f, None))

            cls.add_to_index(obj)

            return obj

        @classmethod
        def bind_test(cls, obj):
            cls.get_from_index(obj.chunk_hid)._tests.append(obj)

        def get_failed_tests(self):
            return filter(lambda r: r.is_failed(), self._tests)

        def get_all_tests(self):
            return self._tests

        def get_html(self):
            return f"""
            Chunk:
            {self.path} {self.name} {self.subtest_name}
            {self.rich_snippet}
            """

        def as_dict(self):
            return dict((k, v) for k, v in self.__dict__.items() if not k.startswith("_"))

    class Test(Base):
        def __init__(self):
            super().__init__()
            self.suite_hid: str
            self.chunk_hid: str
            self.subtest_name: str
            self.log_path: str

        @classmethod
        def from_rec(cls, rec):
            obj = super().from_rec(rec)

            if not obj or "suite_hid" not in rec or "chunk_hid" not in rec:
                return None

            for f in ["suite_hid", "chunk_hid", "subtest_name"]:
                setattr(obj, f, rec.get(f, None))
            setattr(obj, "log_path", rec.get("links", {}).get("log", None))

            cls.add_to_index(obj)

            return obj

        @classmethod
        def ZZZ_get_all_failed_tests(cls):
            return filter(lambda r: r.is_failed(), cls.index.values())

        def get_html(self):
            return f"""
            Test:
            [[[bad]]{self.status}[[rst]]] ({round(self.duration, 1)}s) [[path]]{self.path}[[rst]] [[name]]{self.name}[[rst]]::{self.subtest_name}
            {self.rich_snippet}
            """

        def as_dict(self):
            return self.__dict__

    suites = []

    total_tests = Total(0, 0, 0, 0, 0, 0)
    total_chunks = Total(0, 0, 0, 0, 0, 0)
    total_suites = Total(0, 0, 0, 0, 0, 0)

    Suite.reset_index()
    Chunk.reset_index()
    Test.reset_index()

    for rec in data["results"]:
        # suite = Suite.from_rec(rec)
        suite = Suite.from_rec(rec)
        if suite:
            total_suites.total += 1
            if suite.is_failed():
                total_suites.failed += 1
            elif suite.is_ok():
                total_suites.ok += 1
            elif suite.is_skipped():
                total_suites.skipped += 1
            elif suite.is_not_launched():
                total_suites.not_launched += 1
            else:
                raise suite
            continue

        chunk = Chunk.from_rec(rec)
        if chunk:
            total_chunks.total += 1
            if chunk.is_failed():
                total_chunks.failed += 1
            elif chunk.is_ok():
                total_chunks.ok += 1
            elif chunk.is_skipped():
                total_chunks.skipped += 1
            elif chunk.is_not_launched():
                total_chunks.not_launched += 1
            else:
                raise chunk

            Suite.bind_chunk(chunk)
            continue

        test = Test.from_rec(rec)
        if test:
            total_tests.total += 1
            if test.is_failed():
                total_tests.failed += 1
            elif test.is_ok():
                total_tests.ok += 1
            elif test.is_skipped():
                total_tests.skipped += 1
            elif test.is_not_launched():
                total_tests.not_launched += 1
            else:
                raise test

            Suite.bind_test(test)
            Chunk.bind_test(test)
            continue

    for suite in Suite.get_failed_from_index():
        if not (suite.get_all_tests()):
            total_suites.suite_faield_w_no_tests += 1

    for suite in Suite.get_failed_from_index():
        for test in suite.get_failed_tests():
            chunk = Chunk.get_from_index(test.chunk_hid)

    return {
        "total_suites": total_suites,
        "total_chunks": total_chunks,
        "total_tests": total_tests,
        "all_suites": list(Suite.get_all_from_index()),
        "all_chunks": list(Chunk.get_all_from_index()),
        "all_tests": list(Test.get_all_from_index()),
    }


def upload_prepare_tables():
    @yt.yt_dataclass
    class Build:
        uid: str
        branch: str
        build_type: str
        date: str
        data: typing.Optional[yt.schema.YsonBytes]

    @yt.yt_dataclass
    class Test:
        build_uid: str
        id: str
        status: str
        yt_test_id: str
        is_suite: bool
        data: typing.Optional[yt.schema.YsonBytes]

    client = client = yt.YtClient(STORAGE_CLUSTER, config={"token": YT_TOKEN})

    if not client.exists(STORAGE_TABLE_BUILDS):
        table_schema = yt.schema.TableSchema.from_row_type(Build)
        table_schema.columns[0].sort_order = "ascending"  # uid
        client.create(
            "table",
            STORAGE_TABLE_BUILDS,
            attributes={
                "dynamic": True,
                "schema": table_schema,
                "max_data_ttl": STORAGE_TTL_BUILDS,
                "min_data_versions": 0,
                "min_data_ttl": 0,
            },
        )
        client.mount_table(STORAGE_TABLE_BUILDS, sync=True)

    if not client.exists(STORAGE_TABLE_TESTS):
        table_schema = yt.schema.TableSchema.from_row_type(Test)
        table_schema.columns[0].sort_order = "ascending"  # build_uid
        table_schema.columns[1].sort_order = "ascending"  # id
        client.create(
            "table",
            STORAGE_TABLE_TESTS,
            attributes={
                "dynamic": True,
                "schema": table_schema,
                "max_data_ttl": STORAGE_TTL_TESTS,
                "min_data_versions": 0,
                "min_data_ttl": 0,
            },
        )
        client.mount_table(STORAGE_TABLE_TESTS, sync=True)
    else:
        yt_table_schema = yt.schema.TableSchema.from_yson_type(client.get(STORAGE_TABLE_TESTS + "/@schema"))
        table_schema = yt.schema.TableSchema.from_row_type(Test)
        new_columns = set([c.name for c in table_schema.columns]) - set([c.name for c in yt_table_schema.columns])
        if new_columns:
            print("Try to modify schema")
            client.unmount_table(STORAGE_TABLE_TESTS, sync=True)
            client.alter_table(STORAGE_TABLE_TESTS, schema=table_schema)
            client.mount_table(STORAGE_TABLE_TESTS, sync=True)


def upload_build(meta, total_tests, total_suites, tests, suites, build_info):
    upload_prepare_tables()

    client = client = yt.YtClient(STORAGE_CLUSTER, config={"token": YT_TOKEN})

    client.insert_rows(
        STORAGE_TABLE_BUILDS,
        [
            {
                "uid": build_info["uid"],
                "build_type": build_info["commit"]["repository"],
                "branch": build_info["commit"]["target_branch"],
                "date": build_info["commit"]["date"],
                "data": yt.yson.to_yson_type(build_info),
            },
        ],
    )

    client.insert_rows(
        STORAGE_TABLE_TESTS,
        [
            {
                "is_suite": False,
                "build_uid": build_info["uid"],
                "id": str(t.id),
                "status": str(t.status),
                "yt_test_id": "::".join(["test", t.path, t.name, t.subtest_name]),
                "data": yt.yson.to_yson_type(t.as_dict()),
            }
            for t in tests
        ],
    )

    client.insert_rows(
        STORAGE_TABLE_TESTS,
        [
            {
                "is_suite": True,
                "build_uid": build_info["uid"],
                "id": str(t.id),
                "status": str(t.status),
                "yt_test_id": "::".join(["suite", t.path, t.name]),
                "data": yt.yson.to_yson_type(t.as_dict()),
            }
            for t in suites
        ],
    )


def _extract_meta_from_ci(ci_context, artifacts):
    result = {
        "ci": {
            "id": ci_context["context"]["version"],
            "flow_url": ci_context["context"]["ci_url"],
        },
        "commit": {
            "author": ci_context["context"]["target_commit"]["author"],
            "date": ci_context["context"]["target_commit"]["date"],
            "revision": ci_context["context"]["target_commit"]["revision"]["hash"],
            "message": ci_context["context"]["target_commit"]["message"],
            "github_link": ci_context["context"]["target_commit"]["github_link"],
            "github_comment_link": ci_context["context"]["target_commit"]["github_comment_link"],
            "repository": ci_context["context"]["target_commit"]["repository"],
            "target_branch": ci_context["context"]["target_commit"]["target_branch"],
        },
        "artifacts": artifacts,
    }

    if "flow_vars" in ci_context and "build-type" in ci_context["flow_vars"]:
        # TODO: migrate to this
        result["ci"]["build_type"] = ci_context["flow_vars"]["build-type"]

    return result


def get_build_info(meta, total_tests, total_suites, build_id) -> dict[str, typing.Any]:
    return {
        "uid": str(build_id) if build_id else str(uuid.uuid4()),
        **meta,
        "total_tests": {
            "total": total_tests.total,
            "failed": total_tests.failed,
            "ok": total_tests.ok,
            "skipped": total_tests.skipped,
            "not_launched": total_tests.not_launched,
        },
        "total_suites": {
            "total": total_suites.total,
            "failed": total_suites.failed,
            "faield_w_no_tests": total_suites.suite_faield_w_no_tests,
            "ok": total_suites.ok,
            "skipped": total_suites.skipped,
            "not_launched": total_suites.not_launched,
        },
    }


def main():
    parser = argparse.ArgumentParser(
        prog="Analysis of test results",
        description="Read TEST_ENVIRONMENT_JSON_V2, creates a short report and uploads the results to YT.",
    )

    parser.add_argument(
        "--input-report",
        help="Path to jsons from SB test build (TEST_ENVIRONMENT_JSON_V2)",
        default=[],
        type=str,
        action="append",
        dest="input_report_files",
    )
    parser.add_argument(
        "--no-upload-result",
        help="Do no upload test result to cluster",
        dest="upload_result",
        default=True,
        action="store_false",
    )

    parser.add_argument("--input-ci-context", help="Path to ci context file", dest="ci_context_file", required=True)
    parser.add_argument("--input-artifacts", help="JSON with artifacts list", type=json.loads)

    parser.add_argument("--output-data", help="Path to output data", type=str, required=False)
    parser.add_argument("--build-id", help="If not set, build_id will generate automatically", type=str, required=False)

    args = parser.parse_args()

    with open(args.ci_context_file) as f:
        ci_context = json.load(f)

    meta = _extract_meta_from_ci(ci_context, args.input_artifacts)

    input_report_files = [file for file in args.input_report_files if os.path.exists(file)]
    assert input_report_files, "No existing input files"

    processed_data = None
    for file in input_report_files:
        with open(file, "r") as h:
            data = json.load(h)
        local_processed_data = process_data(data)
        if processed_data:
            for f in ("total_suites", "total_chunks", "total_tests"):
                processed_data[f] += local_processed_data[f]
            for f in ("all_suites", "all_chunks", "all_tests"):
                processed_data[f].extend(local_processed_data[f])
        else:
            processed_data = local_processed_data

    build_info = get_build_info(meta, processed_data["total_tests"], processed_data["total_suites"], args.build_id)

    if args.upload_result:
        upload_build(
            meta=meta,
            total_tests=processed_data["total_tests"],
            total_suites=processed_data["total_suites"],
            tests=processed_data["all_tests"],
            suites=processed_data["all_suites"],
            build_info=build_info,
        )

    if args.output_data:
        with open(args.output_data, "w") as f:
            json.dump(build_info, f, indent=4)


if __name__ == "__main__":
    main()
