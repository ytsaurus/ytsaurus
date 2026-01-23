import os
import pytest
import subprocess
import yatest
import yaml

from pathlib import Path


def get_record_includes():
    try:
        from library.python import resource
        content = resource.find("record_includes.yaml")
    except ImportError:
        record_includes_path = os.path.join(os.path.dirname(__file__), "record_includes.yaml")
        with open(record_includes_path, "rb") as fin:
            content = fin.read()
    return yaml.safe_load(content)


RECORD_CODEGEN_BINARY = os.path.join(yatest.common.binary_path("yt/yt/tools/record_codegen/yt_record_codegen/bin"), "record-codegen")
TEST_RECORDS_PATH = yatest.common.source_path("yt/yt/tools/record_codegen/yt_record_codegen/tests/records")
TEST_RECORDS_FILES = [
    test_record_file for test_record_file in os.listdir(TEST_RECORDS_PATH)
]
RECORD_INCLUDES = get_record_includes()


def run_test(tmpdir, test_record_file: str, output_includes: list[str] = []):
    test_record_file = os.path.join(TEST_RECORDS_PATH, test_record_file)

    output_root: Path = tmpdir / "records"
    output_root.mkdir()

    output_cpp_path = output_root / "record.cpp"
    output_h_path = output_root / "record.h"

    cmd = [
        RECORD_CODEGEN_BINARY,
        "--input", test_record_file,
        "--output-root", output_root,
        "--output-cpp", output_cpp_path,
    ]
    for output_include in output_includes:
        cmd.extend([
            "--output-include",
            output_include,
        ])

    codegen_process_result = subprocess.run(
        cmd,
        stdin=subprocess.DEVNULL)

    if codegen_process_result.returncode != 0:
        raise RuntimeError(f"Codegen process failed with return code {codegen_process_result.returncode}, stderr: {codegen_process_result.stderr}")

    return [
        yatest.common.canonical_file(output_cpp_path, local=True),
        yatest.common.canonical_file(output_h_path, local=True),
    ]


@pytest.mark.parametrize("test_record_file", TEST_RECORDS_FILES)
def test_canonizator(tmpdir, test_record_file):
    return run_test(tmpdir, test_record_file, RECORD_INCLUDES.get(test_record_file, []))
