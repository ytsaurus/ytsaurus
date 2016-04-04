# coding: utf-8

import yatest.common
import mr_utils


def test():
    data_path = '.'
    return mr_utils.mapreduce_test(
        yatest.common.binary_path("quality/blender/wizards_clicks/wizards_clicks"),
        args=[
            'aggregate_old',
            '--server', 'local',
            '--cfg', yatest.common.data_path('quality/blender/wizards_clicks/aggr_cfg.json'),
            '--src', 'raw_clicks/20150907/doppel-word/10k_sample',
            '--dst', 'factors/doppel-word'
        ],
        data_path=data_path,
        input_tables=[
            mr_utils.TableSpec(file_path='raw_clicks.20150907.doppel-word.10k_sample', table_name='raw_clicks/20150907/doppel-word/10k_sample', mapreduce_io_flags=['-fs', ',']),
        ],
        output_tables=[
            mr_utils.TableSpec(file_path="fake", table_name="fake"), # fake table, test fail with only one table
            mr_utils.TableSpec(file_path="doppel-word", table_name="factors/doppel-word"),
        ],
    )
