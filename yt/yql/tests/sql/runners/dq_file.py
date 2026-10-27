import pytest
import re

import yatest.common

from yql_utils import get_supported_providers, is_xfail, is_skip_forceblocks, \
    do_custom_query_check, is_with_final_result_issues, yql_binary_path, get_param, pytest_get_current_part

from test_utils import collect_argvalues_for_run, get_config
from test_file_common import run_file, run_file_no_cache

from yt.yql.tests.sql.runners.common import (
    DATA_PATH,
    add_table_clusters,
    patch_cfg_file,
    read_sql_query,
    resolve_langver,
    skip_if_non_trivial_gateway,
)
from yt.yql.tests.sql.runners.compare import compare_file_run_with_reference

DQRUN_PATH = yql_binary_path('yt/yql/tools/dqrun_light/dqrun_light')
YTFILERUN_PATH = yql_binary_path('yt/yql/tools/ytfilerun/impl/ytfilerun')


def make_test_class(suite, case, cfg):
    class TestCase:
        @pytest.mark.parametrize('what', ['Results', 'ForceBlocks', 'Plan'])
        def test(self, tmpdir, what, yql_http_file_server):
            return run_test(suite, case, cfg, tmpdir, what, yql_http_file_server)

    TestCase.__name__ = 'TestDqFile_{}-{}-{}'.format(suite, case, cfg)
    return TestCase


def collect_test_classes(test_file):
    current_part, part_count = pytest_get_current_part(test_file)
    classes = {}
    for template in ('.sql', '.yql'):
        for suite, case, cfg in collect_argvalues_for_run(template, None, current_part, part_count, DATA_PATH, None):
            cls = make_test_class(suite, case, cfg)
            assert cls.__name__ not in classes, 'Duplicate SQL case: {}'.format(cls.__name__)
            classes[cls.__name__] = cls
    return classes


def run_test(suite, case, cfg, tmpdir, what, yql_http_file_server):
    skip_if_non_trivial_gateway(what)

    if get_param('SQL_FLAGS') and what == 'Plan':
        pytest.skip('SKIP')

    config = get_config(suite, case, cfg, data_path=DATA_PATH)
    cfg_postprocess = add_table_clusters(suite, config)
    patch_cfg_file_path = patch_cfg_file(DATA_PATH, suite, config)
    sql_query = read_sql_query(DATA_PATH, suite, case)

    force_blocks = what == 'ForceBlocks'
    xfail = is_xfail(config)
    langver = resolve_langver(config)

    if what == 'Plan' and xfail:
        pytest.skip('xfail is not supported in this mode')

    if force_blocks:
        if is_skip_forceblocks(config):
            pytest.skip('skip force blocks requested')
        if re.search(r"skip force_blocks", sql_query):
            pytest.skip('skip force blocks requested')
    elif what != 'Plan':
        if not xfail and ('ytfile can not' in sql_query or 'yt' not in get_supported_providers(config)):
            pytest.skip('yqlrun is not supported')

    extra_args = ["--emulate-yt"]
    if is_with_final_result_issues(config):
        extra_args += ["--with-final-issues"]

    (res, tables_res) = run_file(
        'dq', suite, case, cfg, config, yql_http_file_server, DQRUN_PATH,
        extra_args=extra_args, data_path=DATA_PATH,
        cfg_postprocess=cfg_postprocess, langver=langver, patch_cfg_file=patch_cfg_file_path,
    )

    if what == 'Plan':
        return [yatest.common.canonical_file(res.plan_file)]

    if what == 'Results' or force_blocks:
        if not xfail:
            if force_blocks:
                yqlrun_res, yqlrun_tables_res = run_file_no_cache(
                    'dq', suite, case, cfg, config, yql_http_file_server, DQRUN_PATH,
                    extra_args=["--emulate-yt"], force_blocks=True, data_path=DATA_PATH,
                    cfg_postprocess=cfg_postprocess, langver=langver, patch_cfg_file=patch_cfg_file_path,
                )
                primary_name = 'Scalar'
                reference_name = 'Block'
            else:
                yqlrun_res, yqlrun_tables_res = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server, yqlrun_binary=YTFILERUN_PATH, data_path=DATA_PATH,
                                                                  cfg_postprocess=cfg_postprocess, langver=langver, patch_cfg_file=patch_cfg_file_path)
                primary_name = 'DQFILE'
                reference_name = 'YQLRUN'

            if do_custom_query_check(yqlrun_res, sql_query):
                return None

            compare_file_run_with_reference(
                res, tables_res, yqlrun_res, yqlrun_tables_res,
                primary_name=primary_name, reference_name=reference_name,
            )

    else:
        assert False, "Unexpected test mode %(what)s"
