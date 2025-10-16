import argparse
import os
import logging
import sys
import tempfile
from typing import Callable
from pathlib import Path
from jinja2 import Template
from library.python import resource, find_root

from yt.yt.tools.sqllogictest_generator.lib.processor import SQLLogicProcessor
from yt.yt.tools.sqllogictest_generator.lib.executor import SQLiteExecutor
from yt.yt.tools.sqllogictest_generator.lib.adapter import YTSuiteAdapter


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()


SQLITE_DATABASE_NAME = "database"
JINJA_TEMPLATE_DIR = "yt/yt/tools/sqllogictest_generator/jinja/"
JINJA_TEMPLATE_NAME = "ql_sqllogic_ut.cpp.jinja"
GENERATED_UNITTEST_FILE_NAME = "ql_sqllogic_ut.cpp"
SOURCE_FILE_EXTENSION = ".test"
SKIP_FILE_EXTENSION = ".skip.yson"


def traverse(directory: Path, callback: Callable[[Path, str], None]) -> None:
    for dirpath, dirnames, filenames in directory.walk():
        logger.info(f"Entering directory {dirpath}")

        if dirnames:
            for dirname in dirnames:
                traverse(dirname, callback)
        if filenames:
            for filename in filenames:
                if filename.endswith(SOURCE_FILE_EXTENSION):
                    logger.info(f"Running callback for {filename}")
                    callback(dirpath, filename)


def main(args: argparse.Namespace) -> None:
    source_path = Path(args.source_path)
    output_path = Path(args.output_path)
    unittest_output_path = Path(args.unittest_output_path)
    arcadia_root = find_root.detect_root(os.getcwd())

    if not source_path.exists():
        raise RuntimeError(f"Provided source path {args.source_path} not exists")

    with tempfile.TemporaryDirectory() as tmp_dir_str:
        db_path = Path(tmp_dir_str) / SQLITE_DATABASE_NAME
        processed_suites = []

        def generate_suite(dirpath: Path, filename: str) -> None:
            suite_name = os.path.splitext(os.path.basename(filename))[0]
            destination_path = output_path / dirpath.relative_to(source_path) / f"generated_suite_{suite_name}.yson"

            skip_source_path = None
            if args.skip_source_path:
                skip_source_path = (
                    Path(args.skip_source_path)
                    / dirpath.relative_to(source_path)
                    / f"{suite_name}{SKIP_FILE_EXTENSION}"
                )

            processed_suites.append((suite_name, f"/{destination_path.relative_to(arcadia_root)}"))

            if destination_path.exists():
                logger.info(f"Previously generated output file for source file {filename} exists: {destination_path}")
                if not args.override_existing:
                    logger.info(f"To override {destination_path} output file add --override-existing parameter")
                    return
                logger.info(f"Overriding {destination_path} output file")

            with SQLiteExecutor(db_path) as executor:
                processor = SQLLogicProcessor(executor, YTSuiteAdapter)
                processor.processs(
                    source_path=dirpath / filename,
                    destination_path=destination_path,
                    skip_source_path=skip_source_path,
                )

        def generate_unittest() -> None:
            logger.info("Updating unittests file from Jinja template")

            template_content = resource.find(JINJA_TEMPLATE_NAME).decode("utf-8")
            assert template_content

            template = Template(template_content)
            rendered_output = template.render({"suites": processed_suites})

            with open(unittest_output_path / GENERATED_UNITTEST_FILE_NAME, "w") as f:
                f.write(rendered_output)

        traverse(source_path, generate_suite)
        generate_unittest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate sqllogic intergation tests")
    parser.add_argument(
        "-e",
        "--override-existing",
        dest="override_existing",
        action="store_true",
        help="Override existing generated yson files",
    )
    parser.add_argument(
        "-i",
        "--input-source-path",
        dest="source_path",
        type=str,
        required=True,
        help="Path to directory with SQL queries",
    )
    parser.add_argument(
        "-s",
        "--skip-source-path",
        dest="skip_source_path",
        type=str,
        help="Path to directory with skipped queries",
    )
    parser.add_argument(
        "-o",
        "--output-path",
        dest="output_path",
        type=str,
        required=True,
        help="Path to directory where generate YSON",
    )
    parser.add_argument(
        "-u",
        "--unittest-output-path",
        dest="unittest_output_path",
        type=str,
        required=True,
        help="Path to directory where generate unittests",
    )
    args = parser.parse_args()
    main(args)
