from pathlib import Path

import yatest.common

from yt.yt.tools.sqllogictest_generator.lib.processor import SQLLogicProcessor
from yt.yt.tools.sqllogictest_generator.lib.executor import SQLiteExecutor
from yt.yt.tools.sqllogictest_generator.lib.adapter import YTSuiteAdapter


def test_processor():
    source_path = Path(yatest.common.source_path("yt/yt/tools/sqllogictest_generator/lib/test/testdata/select1.test"))
    skip_source_path = Path(yatest.common.source_path("yt/yt/tools/sqllogictest_generator/lib/test/testdata/select1.skip.yson"))
    destination_path = Path(yatest.common.output_path()) / "output.yson"
    db_path = Path(yatest.common.output_path()) / "database"

    with SQLiteExecutor(db_path) as executor:
        processor = SQLLogicProcessor(executor, YTSuiteAdapter)
        processor.processs(source_path, destination_path, skip_source_path)

    return yatest.common.canonical_file(str(destination_path), local=True)
