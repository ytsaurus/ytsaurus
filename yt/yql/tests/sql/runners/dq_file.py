import pytest
import re

from yql_utils import get_supported_providers, is_xfail, is_skip_forceblocks, \
    do_custom_query_check, is_with_final_result_issues, yql_binary_path

from test_utils import get_config
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

DQRUN_PATH = yql_binary_path('yt/yql/tools/dqrun/dqrun')


def run_test(suite, case, cfg, tmpdir, what, yql_http_file_server):
    skip_if_non_trivial_gateway(what)

    config = get_config(suite, case, cfg, data_path=DATA_PATH)
    cfg_postprocess = add_table_clusters(suite, config)
    patch_cfg_file_path = patch_cfg_file(DATA_PATH, suite, config)
    sql_query = read_sql_query(DATA_PATH, suite, case)

    force_blocks = what == 'ForceBlocks'
    xfail = is_xfail(config)
    langver = resolve_langver(config)

    if force_blocks:
        if is_skip_forceblocks(config):
            pytest.skip('skip force blocks requested')
        if re.search(r"skip force_blocks", sql_query):
            pytest.skip('skip force blocks requested')
    else:
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
                yqlrun_res, yqlrun_tables_res = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server, data_path=DATA_PATH,
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
