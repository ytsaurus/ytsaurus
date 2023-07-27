import os
import tempfile

import yatest.common
from library.python import json
from library.python import fs


def _get_temp_file_name():
    _, file_name = tempfile.mkstemp(
        suffix=".json",
        dir=yatest.common.runtime.work_path(),
    )
    return file_name


def test_read_json():
    file_name = _get_temp_file_name()
    fs.write_file(file_name, "{\"a\":100}")
    contents = json.read_file(file_name)
    assert contents["a"] == 100
    os.unlink(file_name)


def test_write_json():
    contents = {"a": 100}
    file_name = _get_temp_file_name()

    # test default serialization
    json.write_file(file_name, contents)
    assert fs.read_file_unicode(file_name) == "{\"a\": 100}"

    # test indentation
    json.write_file(file_name, contents, indent=4)
    assert fs.read_file_unicode(file_name) == "{\n    \"a\": 100\n}"
