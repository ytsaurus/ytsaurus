import os
import pytest
import re

import yatest.common
from yql_utils import execute, get_tables, get_files, get_http_files, \
    KSV_ATTR, yql_binary_path, is_xfail, is_canonize_peephole, is_peephole_use_blocks, is_canonize_lineage, \
    is_skip_forceblocks, get_param, normalize_source_code_path, \
    do_custom_query_check, stable_result_file, stable_table_file, is_with_final_result_issues
from yqlrun import YQLRun

from test_utils import get_config, get_parameters_json
from test_file_common import run_file, run_file_no_cache, get_gateways_config

from yt.yql.tests.common.test_framework.test_utils import infer_yt_schema

from yt.yql.tests.sql.runners.common import (
    DATA_PATH,
    add_table_clusters,
    iter_out_tables_for_canon,
    maybe_append_table_attr_canon,
    patch_cfg_file,
    read_sql_query,
    resolve_langver,
    skip_if_non_trivial_gateway,
)

ASTDIFF_PATH = yql_binary_path('yql/essentials/tools/astdiff/astdiff')
YSONDIFF_PATH = yql_binary_path('yql/essentials/tools/ysondiff/ysondiff')


def file_diff(expected_file, actual_file):
    return '\n'.join([
        'Actual result:',
        ' %s\n' % (actual_file),
        'Expected result:',
        ' %s\n' % (expected_file),
    ])


def check_result(sql_query, expected, actual):
    (expected, expected_tables) = expected
    (actual, actual_tables) = actual

    if do_custom_query_check(expected, sql_query):
        return

    if os.path.exists(actual.results_file):
        assert os.path.exists(expected.results_file)

        expected_file = stable_result_file(expected)
        actual_file = stable_result_file(actual)

        assert expected_file == actual_file, '\n'.join([
            'RESULTS_DIFFER',
            file_diff(expected_file, actual_file),
        ])

    for table in expected_tables:
        if not os.path.exists(expected_tables[table].file):
            continue
        assert os.path.exists(actual_tables[table].file)

        expected_file = stable_table_file(expected_tables[table])
        actual_file = stable_table_file(actual_tables[table])

        assert expected_file == actual_file, '\n'.join([
            'RESULTS_DIFFER FOR TABLE %s' % (table),
            file_diff(expected_file, actual_file),
        ])


def run_test(suite, case, cfg, tmpdir, what, yql_http_file_server):
    if get_param('SQL_FLAGS'):
        if what == 'Debug' or what == 'Plan' or what == 'Peephole' or what == 'Lineage':
            pytest.skip('SKIP')
    skip_if_non_trivial_gateway(what)

    ytfilerun_binary = yql_binary_path('yt/yql/tools/ytfilerun/ytfilerun')
    config = get_config(suite, case, cfg, data_path=DATA_PATH)
    cfg_postprocess = add_table_clusters(suite, config)
    patch_cfg_file_path = patch_cfg_file(DATA_PATH, suite, config)
    sql_query = read_sql_query(DATA_PATH, suite, case)

    xfail = is_xfail(config)
    if xfail and what != 'Results':
        pytest.skip('xfail is not supported in this mode')

    langver = resolve_langver(config)

    if what == 'Peephole':
        canonize_peephole = is_canonize_peephole(config)
        if not canonize_peephole:
            canonize_peephole = re.search(r"canonize peephole", sql_query)
            if not canonize_peephole:
                pytest.skip('no peephole canonization requested')

        force_blocks = is_peephole_use_blocks(config)
        (res, tables_res) = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server, yqlrun_binary=ytfilerun_binary, force_blocks=force_blocks,
                                              extra_args=['--peephole'], data_path=DATA_PATH, cfg_postprocess=cfg_postprocess, langver=langver, attr_postprocess=infer_yt_schema,
                                              patch_cfg_file=patch_cfg_file_path)
        return [yatest.common.canonical_file(res.opt_file, diff_tool=ASTDIFF_PATH)]

    if what == 'Lineage':
        canonize_lineage = is_canonize_lineage(config)
        if not canonize_lineage:
            pytest.skip('no lineage canonization requested')

        (res, tables_res) = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server, yqlrun_binary=ytfilerun_binary,
                                              extra_args=['--lineage'], data_path=DATA_PATH, cfg_postprocess=cfg_postprocess, langver=langver, attr_postprocess=infer_yt_schema,
                                              patch_cfg_file=patch_cfg_file_path)
        return [yatest.common.canonical_file(res.results_file)]

    if what == 'PartialTypeCheck':
        if re.search(r"skip partial typecheck", sql_query):
            pytest.skip('no partial typecheck test requested')

        (res, tables_res) = run_file_no_cache(
            'yt', suite, case, cfg, config, yql_http_file_server, yqlrun_binary=ytfilerun_binary,
            extra_args=["--compile-only", "--test-partial-typecheck"],
            data_path=DATA_PATH, cfg_postprocess=cfg_postprocess, langver=langver,
            attr_postprocess=infer_yt_schema, patch_cfg_file=patch_cfg_file_path,
        )
        return None

    extra_final_args = []
    if is_with_final_result_issues(config):
        extra_final_args += ['--with-final-issues']
    (res, tables_res) = run_file('yt', suite, case, cfg, config, yql_http_file_server, yqlrun_binary=ytfilerun_binary,
                                 extra_args=extra_final_args, data_path=DATA_PATH, cfg_postprocess=cfg_postprocess, langver=langver, attr_postprocess=infer_yt_schema,
                                 patch_cfg_file=patch_cfg_file_path)

    to_canonize = []

    if what == 'Results':
        if xfail:
            return None

        if do_custom_query_check(res, sql_query):
            return None

        if os.path.exists(res.results_file):
            stable_result_file(res)
            to_canonize.append(yatest.common.canonical_file(res.results_file))
        _, out_tables = get_tables(suite, config, DATA_PATH, def_attr=KSV_ATTR, attr_postprocess=infer_yt_schema)
        for table in iter_out_tables_for_canon(out_tables, tables_res):
            table_res = tables_res[table.full_name]
            if os.path.exists(table_res.file):
                stable_table_file(table_res)
                to_canonize.append(yatest.common.canonical_file(table_res.file, diff_tool=YSONDIFF_PATH))
                maybe_append_table_attr_canon(to_canonize, table_res.yqlrun_file, diff_tool=YSONDIFF_PATH)
        if res.std_err:
            to_canonize.append(normalize_source_code_path(res.std_err))

    if what == 'Plan':
        to_canonize = [yatest.common.canonical_file(res.plan_file)]

    if what == 'Debug':
        to_canonize = [yatest.common.canonical_file(res.opt_file, diff_tool=ASTDIFF_PATH)]

    if what == 'RunOnOpt':
        in_tables, out_tables = get_tables(suite, config, DATA_PATH, def_attr=KSV_ATTR, attr_postprocess=infer_yt_schema)
        files = get_files(suite, config, DATA_PATH)
        http_files = get_http_files(suite, config, DATA_PATH)
        http_files_urls = yql_http_file_server.register_files({}, http_files)
        parameters = get_parameters_json(suite, config, DATA_PATH)

        yqlrun = YQLRun(
            prov='yt',
            keep_temp=False,
            gateway_config=get_gateways_config(http_files, yql_http_file_server, postprocess_func=cfg_postprocess),
            binary=ytfilerun_binary,
            udfs_dir=yql_binary_path('yql/essentials/tests/common/test_framework/udfs_deps'),
            langver=langver,
            patch_cfg_file=patch_cfg_file_path,
        )

        opt_res, opt_tables_res = execute(
            yqlrun,
            program=res.opt,
            input_tables=in_tables,
            output_tables=out_tables,
            files=files,
            urls=http_files_urls,
            check_error=True,
            verbose=True,
            parameters=parameters)

        if os.path.exists(res.results_file):
            assert res.results == opt_res.results
        for table in tables_res:
            if os.path.exists(tables_res[table].file):
                assert tables_res[table].content == opt_tables_res[table].content

        check_plan = True
        check_ast = False  # Temporary disable
        if re.search(r"ignore runonopt ast diff", sql_query):
            check_ast = False
        if re.search(r"ignore runonopt plan diff", sql_query):
            check_plan = False

        if check_plan:
            assert res.plan == opt_res.plan
        if check_ast:
            yatest.common.process.execute([ASTDIFF_PATH, res.opt_file, opt_res.opt_file], check_exit_code=True)

        return None

    if what == 'ForceBlocks':
        skip_forceblocks = is_skip_forceblocks(config) or re.search(r"skip force_blocks", sql_query)
        if skip_forceblocks:
            pytest.skip('no force_blocks test requested')

        blocks_res, blocks_tables_res = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server, yqlrun_binary=ytfilerun_binary,
                                                          force_blocks=True, data_path=DATA_PATH, cfg_postprocess=cfg_postprocess, langver=langver,
                                                          attr_postprocess=infer_yt_schema, patch_cfg_file=patch_cfg_file_path)

        check_result(sql_query, expected=(res, tables_res), actual=(blocks_res, blocks_tables_res))
        return None

    if what == 'AutoYqlSelect':
        yql_select_res, yql_select_tables_res = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server, yqlrun_binary=ytfilerun_binary,
                                                                  force_blocks=True, data_path=DATA_PATH, cfg_postprocess=cfg_postprocess, langver=langver,
                                                                  attr_postprocess=infer_yt_schema, is_yql_select=True,
                                                                  patch_cfg_file=patch_cfg_file_path)

        check_result(sql_query, expected=(res, tables_res), actual=(yql_select_res, yql_select_tables_res))
        return None

    return to_canonize
