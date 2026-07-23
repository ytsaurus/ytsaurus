import codecs
import json
import os
import re
import pytest
import yatest.common
from yt import yson
import cyson

from yql_utils import KSV_ATTR, execute_sql, get_supported_providers, get_test_prefix, log, replace_vals, is_xfail, \
    yql_binary_path, normalize_yson, normalize_plan_ids, normalize_source_code_path, get_tables, \
    get_files, get_http_files, get_yt_files, get_pragmas, is_canonize_yt, skip_test_if_required, do_custom_error_check, \
    get_gateway_cfg_patch, normalize_result, dump_table_yson

from test_utils import get_config, get_parameters_json, get_case_file, replace_vars

from yqlrun import YQLRun

from yt.yson.convert import yson_to_json
from yt.yql.tests.common.test_framework.test_utils import infer_yt_schema

from yt.yql.tests.sql.runners.common import DATA_PATH, resolve_langver, maybe_append_table_attr_canon, iter_out_tables_for_canon
from yt.yql.tests.sql.runners.yt_setup import upload_yt_files

YTFILERUN_PATH = yql_binary_path('yt/yql/tools/ytfilerun/ytfilerun')
YSONDIFF_PATH = yql_binary_path('yql/essentials/tools/ysondiff/ysondiff')


def _prepare_result_for_custom_check(res_bytes):
    res = cyson.loads(res_bytes) if res_bytes else cyson.loads(b"[]")
    res = replace_vals(res)
    for r in res:
        for data in r[b'Write']:
            if b'Unordered' in r and b'Data' in data:
                data[b'Data'] = sorted(data[b'Data'], key=cyson.dumps)
    return res


def _is_unordered_result_bytes(res_bytes):
    res = cyson.loads(res_bytes)
    for r in res:
        if b'Unordered' in r:
            return True
        for data in r[b'Write']:
            if b'Unordered' in data:
                return True
    return False


def run_test(provider, prepare, suite, case, cfg, tmpdir, tmpdir_module, mongo, yt, yql_api):
    yql_api.default_provider = yt
    yql_api.httpd.forget_files()

    config = get_config(suite, case, cfg, data_path=DATA_PATH)
    skip_test_if_required(config)

    if provider not in get_supported_providers(config):
        pytest.skip('%s provider is not supported here' % provider)

    log('===' + suite + '-' + case + '-' + cfg)

    files = get_files(suite, config, DATA_PATH)
    http_files = get_http_files(suite, config, DATA_PATH)
    http_files_urls = yql_api.httpd.register_files({}, http_files)
    pragmas = get_pragmas(config)

    yt_files = get_yt_files(suite, config, DATA_PATH)
    upload_yt_files(yt, yt_files)

    sql_file = get_case_file(DATA_PATH, suite, case)
    with codecs.open(sql_file, encoding='utf-8') as program_file_descr:
        sql_query = prepare(program_file_descr.read())

    pragmas.append(sql_query)
    sql_query = ';\n'.join(pragmas)
    sql_query = replace_vars(sql_query, 'yt_local_var')

    in_tables, out_tables = get_tables(suite, config, DATA_PATH, def_attr=KSV_ATTR, attr_postprocess=infer_yt_schema)
    xfail = is_xfail(config)
    langver = resolve_langver(config)

    parameters = get_parameters_json(suite, config, DATA_PATH)
    no_plan_diff = provider == 'dq'
    if not no_plan_diff:
        no_plan_diff = re.search(r"/\* ignore plan diff .*\*/", sql_query)
    if provider + ' can not' in sql_query:
        pytest.skip(provider + ' can not execute this')
    if no_plan_diff:
        log('will ignore plan differences')
    no_detailed = re.search(r"/\* ignore yt detailed plan diff .*\*/", sql_query)
    if no_detailed:
        log('will ignore detailed plan differences')

    assert not get_gateway_cfg_patch(config), 'Patching yt test is not supported yet'
    # YT run
    yql_api.set_table_prefix('//' + get_test_prefix() + '/')

    yt_res, yt_tables_res = execute_sql(
        yql_api,
        program=sql_query,
        langver=langver,
        input_tables=in_tables,
        output_tables=out_tables,
        files=files,
        urls=http_files_urls,
        check_error=not xfail,
        verbose=True,
        pretty_plan=False,
        parameters=parameters)

    if xfail:
        log('XFail errors: ' + yt_res.std_err)
        do_custom_error_check(yt_res, sql_query)
        return None

    yt_res_yson = yt_res.results.get('data', [])

    if any([table.format != 'yson' for table in in_tables]) or 'ytfile can not' in sql_query or 'yt' not in get_supported_providers(config):
        to_canonize = []
        if os.path.exists(yt_res.results_file):
            with open(yt_res.results_file, 'w') as f:
                f.write(json.dumps(yson_to_json(yt_res_yson), sort_keys=True, ensure_ascii=False, indent=4))
            to_canonize.append(yatest.common.canonical_file(yt_res.results_file))
        for table in iter_out_tables_for_canon(out_tables, yt_tables_res):
            table_res = yt_tables_res[table.full_name]
            if os.path.exists(table_res.file):
                to_canonize.append(yatest.common.canonical_file(table_res.file, diff_tool=YSONDIFF_PATH))

        if yt_res.std_err:
            to_canonize.append(normalize_source_code_path(yt_res.std_err))

        return to_canonize

    yt_res_bytes = yson.dumps(yt_res_yson)
    custom_check = re.search(r"/\* custom check:(.*)\*/", sql_query)
    if custom_check:
        custom_check = custom_check.group(1)
        yt_res_yson_for_check = _prepare_result_for_custom_check(yt_res_bytes)
    yt_res_yson = normalize_result(yt_res_bytes, _is_unordered_result_bytes(yt_res_bytes))

    # File run
    yqlrun = YQLRun(
        prov='yt',
        keep_temp=not re.search(r"yt\.ReleaseTempData", sql_query),
        gateway_config=str(yql_api.gateway_config),
        binary=YTFILERUN_PATH,
        langver=langver,
    )
    file_res, file_tables_res = execute_sql(
        yqlrun,
        program=sql_query,
        input_tables=in_tables,
        output_tables=out_tables,
        files=files,
        urls=http_files_urls,
        verbose=True,
        parameters=parameters)

    file_res_bytes = file_res.results if file_res.results else b'[]'
    file_res_yson = normalize_result(
        file_res_bytes,
        _is_unordered_result_bytes(file_res_bytes),
    )

    if custom_check:
        log('custom check: ' + custom_check)
        assert eval(custom_check, {}, {'yt_res_yson': yt_res_yson_for_check}), \
            'Condition "%(custom_check)s" fails\n' \
            '%(provider)s result:\n %(yt_res_yson)s\n' % {
                'custom_check': custom_check,
                'provider': provider,
                'yt_res_yson': yt_res_yson_for_check,
            }
    else:
        # Compare results
        assert yt_res_yson == file_res_yson, 'RESULTS_DIFFER\n' \
            '%(provider)s result:\n %(yt_res_yson)s\n\n' \
            'YQLRUN result:\n %(file_res_yson)s\n' % locals()

    # Compare output tables
    for table in file_tables_res:
        assert table in yt_tables_res

        file_table_yson = dump_table_yson(file_tables_res[table].content, sort=True)
        yt_table_yson = dump_table_yson(yt_tables_res[table].content, sort=True)

        assert file_table_yson == yt_table_yson, \
            'OUT_TABLE_DIFFER: %(table)s\n' \
            'YT table:\n %(yt_table_yson)s\n\n' \
            'YQLRUN table:\n %(file_table_yson)s\n' % locals()

    yt_res_plan = json.loads(yt_res.plan)
    yt_res_plan = json.dumps(normalize_plan_ids(yt_res_plan, no_detailed), sort_keys=True, ensure_ascii=False)

    if not no_plan_diff:
        # Check PLAN
        file_plan = file_res.plan
        if isinstance(file_plan, str):
            file_plan = file_plan.encode('utf-8')
        file_res_plan = yson_to_json(normalize_yson(yson.loads(file_plan)))
        file_res_plan = json.dumps(normalize_plan_ids(file_res_plan, no_detailed), sort_keys=True, ensure_ascii=False)
        assert yt_res_plan == file_res_plan, 'PLAN_DIFFER\n' \
            '%(provider)s plan:\n %(yt_res_plan)s\n\n' \
            'YQLRUN plan:\n %(file_res_plan)s\n' % locals()

    # Check AST
    # diff_result = yatest.common.process.execute([ASTDIFF_PATH, file_res.opt_file, yt_res.opt_file], check_exit_code=False)
    # assert diff_result.exit_code == 0, 'AST DIFFER:\n' + diff_result.std_err

    if yt_res.std_err:
        return normalize_source_code_path(yt_res.std_err)

    if provider == 'yt' and is_canonize_yt(config):
        to_canonize = []
        for table in iter_out_tables_for_canon(out_tables, yt_tables_res):
            table_res = yt_tables_res[table.full_name]
            if os.path.exists(table_res.file):
                to_canonize.append(yatest.common.canonical_file(table_res.file, diff_tool=YSONDIFF_PATH))
            maybe_append_table_attr_canon(to_canonize, table_res.yqlrun_file, diff_tool=YSONDIFF_PATH)
        return to_canonize
