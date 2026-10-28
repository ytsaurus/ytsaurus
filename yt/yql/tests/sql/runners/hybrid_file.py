import pytest

from yql_utils import is_xfail, yql_binary_path, pytest_get_current_part, get_udfs

from test_utils import collect_argvalues_for_run, get_config
from test_file_common import run_file, run_file_no_cache

from yt.yql.tests.sql.runners.common import (
    DATA_PATH,
    add_table_clusters,
    patch_cfg_file,
    resolve_langver,
    skip_if_non_trivial_gateway,
)
from yt.yql.tests.sql.runners.compare import compare_file_run_with_reference

DQRUN_PATH = yql_binary_path('yt/yql/tools/dqrun_light/dqrun_light')
YTFILERUN_PATH = yql_binary_path('yt/yql/tools/ytfilerun/impl/ytfilerun')


def make_test_class(suite, case, cfg):
    class TestCase:
        @pytest.mark.parametrize('what', ['Results'])
        def test(self, tmpdir, what, yql_http_file_server):
            return run_test(suite, case, cfg, tmpdir, what, yql_http_file_server)

    TestCase.__name__ = 'TestHybridFile_{}-{}-{}'.format(suite, case, cfg)
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

    config = get_config(suite, case, cfg, data_path=DATA_PATH)
    cfg_postprocess = add_table_clusters(suite, config)
    patch_cfg_file_path = patch_cfg_file(DATA_PATH, suite, config)
    xfail = is_xfail(config)
    langver = resolve_langver(config)

    res, tables_res = run_file(
        'hybrid',
        suite,
        case,
        cfg,
        config,
        yql_http_file_server,
        DQRUN_PATH,
        extra_args=["--emulate-yt", "--no-force-dq"],
        data_path=DATA_PATH,
        cfg_postprocess=cfg_postprocess,
        langver=langver,
        patch_cfg_file=patch_cfg_file_path,
        udfs=get_udfs(config),
    )

    if what == 'Results':
        if not xfail:
            yqlrun_res, yqlrun_tables_res = run_file_no_cache(
                'yt',
                suite,
                case,
                cfg,
                config,
                yql_http_file_server,
                yqlrun_binary=YTFILERUN_PATH,
                data_path=DATA_PATH,
                cfg_postprocess=cfg_postprocess,
                langver=langver,
                patch_cfg_file=patch_cfg_file_path,
                udfs=get_udfs(config),
            )

            compare_file_run_with_reference(
                res,
                tables_res,
                yqlrun_res,
                yqlrun_tables_res,
                primary_name='HYBRIDFILE',
                reference_name='YQLRUN',
            )

    else:
        assert False, "Unexpected test mode %(what)s"
