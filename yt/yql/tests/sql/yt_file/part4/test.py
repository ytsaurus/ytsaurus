import pytest
import yatest

from yt_file import run_test, DATA_PATH
from test_utils import pytest_generate_tests_for_part
from yql_utils import pytest_get_current_part


def pytest_generate_tests(metafunc):
    current_part, part_count = pytest_get_current_part(yatest.common.source_path(__file__))
    return pytest_generate_tests_for_part(metafunc, current_part, part_count, data_path=DATA_PATH)


@pytest.mark.parametrize('what', [
    'Results',
    'Plan',
    'Debug',
    'RunOnOpt',
    'Peephole',
    'Lineage',
    'ForceBlocks',
    'PartialTypeCheck',
    # TODO(YQL-20436): Enable when it is working well
    # 'AutoYqlSelect',
])
def test(suite, case, cfg, tmpdir, what, yql_http_file_server):
    return run_test(suite, case, cfg, tmpdir, what, yql_http_file_server)
