from yql_utils import is_xfail, yql_binary_path

from test_utils import get_config
from test_file_common import run_file, run_file_no_cache

from yt.yql.tests.sql.runners.common import (
    DATA_PATH,
    add_table_clusters,
    resolve_langver,
    skip_if_non_trivial_gateway,
)
from yt.yql.tests.sql.runners.compare import compare_file_run_with_reference

DQRUN_PATH = yql_binary_path('yt/yql/tools/dqrun/dqrun')


def run_test(suite, case, cfg, tmpdir, what, yql_http_file_server):
    skip_if_non_trivial_gateway(what)

    config = get_config(suite, case, cfg, data_path=DATA_PATH)
    cfg_postprocess = add_table_clusters(suite, config)
    xfail = is_xfail(config)
    langver = resolve_langver(config)

    (res, tables_res) = run_file(
        'hybrid', suite, case, cfg, config, yql_http_file_server, DQRUN_PATH,
        extra_args=["--emulate-yt", "--no-force-dq"],
        data_path=DATA_PATH, cfg_postprocess=cfg_postprocess, langver=langver,
    )

    if what == 'Results':
        if not xfail:
            yqlrun_res, yqlrun_tables_res = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server, data_path=DATA_PATH,
                                                              cfg_postprocess=cfg_postprocess, langver=langver)

            compare_file_run_with_reference(
                res, tables_res, yqlrun_res, yqlrun_tables_res,
                primary_name='HYBRIDFILE', reference_name='YQLRUN',
            )

    else:
        assert False, "Unexpected test mode %(what)s"
