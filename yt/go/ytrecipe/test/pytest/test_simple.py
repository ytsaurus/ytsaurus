import subprocess
import pytest

import yatest.common


def test_simple():
    subprocess.check_call([yatest.common.gdb_path(), "--version"])
    pytest.skip("test is working")
