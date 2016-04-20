# coding: utf-8

import yatest.common
import yt_utils


def test():
    data_path = '.'
    return yt_utils.yt_test(
        yatest.common.binary_path("quality/blender/wizards_clicks/wizards_clicks"),
        args=[
            'collect',
            '-g', 'dopp_query',
            '-g', 'dopp_word',
            '-g', 'norm_bigram',
            '-c', 'wiz',
            '-c', 'pos_wiz',
            '-c', 'nav',
            '-c', 'common',
            '-u', 'user_sessions_sample',
            '--cnts_cfg', yatest.common.data_path('quality/blender/wizards_clicks/cnts_cfg.json'),
            '-d', '20150907',
            '--server', 'local',
            '--bsdict', yatest.common.data_path('quality/blender/wizards_clicks/blockstat.dict'),
            '--geodata', yatest.common.data_path('geo/geodata3.bin'),
            '--wiz_lang_dir', yatest.common.data_path('wizard/language'),
            '--wiz_syn_dir', yatest.common.data_path('wizard/synnorm'),
            '--dst', 'daily_sample'
        ],
        data_path=data_path,
        input_tables=[
            mr_utils.TableSpec(file_path='user_sessions.20150907.20k_sample', table_name='user_sessions_sample/20150907', mapreduce_io_flags=['-subkey'])
        ],
        output_tables=[
            mr_utils.TableSpec(file_path="dopp_query", table_name="daily_sample/dopp_query/20150907", mapreduce_io_flags=['-subkey']),
            mr_utils.TableSpec(file_path="dopp_word", table_name="daily_sample/dopp_word/20150907", mapreduce_io_flags=['-subkey']),
            mr_utils.TableSpec(file_path="dopp_pair", table_name="daily_sample/norm_bigram/20150907", mapreduce_io_flags=['-subkey']),
        ],
    )
