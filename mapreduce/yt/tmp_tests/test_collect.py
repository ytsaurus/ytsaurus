# coding: utf-8

import yatest.common
import mr_utils


def test():
    data_path = '.'
    return mr_utils.mapreduce_test(
        yatest.common.binary_path("quality/blender/wizards_clicks/wizards_clicks"),
        args=[
            'collect_old',
            '-g', 'old_dopp_query',
            '-g', 'old_dopp_word',
            '-c', 'wiz', # stub
            '--server', 'local',
            '--bsdict', yatest.common.data_path('quality/blender/wizards_clicks/blockstat.dict'),
            '--src', 'user_sessions/20150907/20k_sample',
            '--dst', 'raw_clicks'
        ],
        data_path=data_path,
        input_tables=[
            mr_utils.TableSpec(file_path='user_sessions.20150907.20k_sample', mapreduce_io_flags=['-subkey'])
        ],
        output_tables=[
            mr_utils.TableSpec(file_path="old_dopp_query", table_name="raw_clicks/old_dopp_query", mapreduce_io_flags=['-subkey']),
            mr_utils.TableSpec(file_path="old_dopp_word", table_name="raw_clicks/old_dopp_word", mapreduce_io_flags=['-subkey']),
        ],
    )
