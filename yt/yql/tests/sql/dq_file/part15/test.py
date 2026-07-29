import pytest
import yatest

from yt.yql.tests.sql.runners.dq_file import run_test
from yt.yql.tests.sql.runners.common import DATA_PATH
from test_utils import pytest_generate_tests_for_part
from yql_utils import pytest_get_current_part


def pytest_generate_tests(metafunc):
    current_part, part_count = pytest_get_current_part(yatest.common.source_path(__file__))
    return pytest_generate_tests_for_part(metafunc, current_part, part_count, data_path=DATA_PATH)


@pytest.mark.parametrize('what', ['Results', 'ForceBlocks'])
def test(suite, case, cfg, tmpdir, what, yql_http_file_server):
    return run_test(suite, case, cfg, tmpdir, what, yql_http_file_server)
