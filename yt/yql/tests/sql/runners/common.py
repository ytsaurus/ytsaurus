import codecs
import os

import pytest
import yatest.common

from yql_utils import get_gateway_cfg_suffix, get_gateway_cfg_patch, get_langver, get_table_clusters
from test_utils import get_case_file

SUITES_PATH = 'yt/yql/tests/sql/suites'
DATA_PATH = yatest.common.source_path(SUITES_PATH)

DEFAULT_LANG_VER = '2025.01'


def resolve_langver(config):
    langver = get_langver(config)
    if langver is None:
        return DEFAULT_LANG_VER
    return langver


def read_sql_query(data_path, suite, case):
    program_sql = get_case_file(data_path, suite, case)
    with codecs.open(program_sql, encoding='utf-8') as program_file:
        return program_file.read()


def patch_cfg_file(data_path, suite, config):
    patch_name = get_gateway_cfg_patch(config)
    if patch_name:
        return os.path.join(data_path, suite, patch_name)
    return None


def skip_if_non_trivial_gateway(what):
    if get_gateway_cfg_suffix() != '' and what != 'Results':
        pytest.skip('non-trivial gateways.conf')


def add_table_clusters(suite, config, data_path=DATA_PATH):
    clusters = get_table_clusters(suite, config, data_path)
    if not clusters:
        return None

    def patch(cfg_message):
        for c in sorted(clusters):
            mapping = cfg_message.Yt.ClusterMapping.add()
            mapping.Name = c
    return patch
