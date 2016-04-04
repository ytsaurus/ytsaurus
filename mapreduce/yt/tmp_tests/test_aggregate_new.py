# coding: utf-8

import yatest.common
import mr_utils


def test():
    data_path = '.'
    return mr_utils.mapreduce_test(
        yatest.common.binary_path("quality/blender/wizards_clicks/wizards_clicks"),
        args=[
            'aggregate',
            '-g', 'dopp_word',
            '--max_threads', '2',
            '--batch_size', '1',
            '-t', '2',
            '--src', 'cnts_daily',
            '--dst', 'cnts_aggr',
            '-d', '20150920-20150921',
            '--server', 'local',
        ],
        data_path=data_path,
        # tables generate example: mapreduce-dev -read tmp/blender/wiz_clicks/daily_no_filter/dopp_query/20151020:[1000000,] -count 10000 -fs , > cnts_daily.dopp_query.20151020
        input_tables=[
            mr_utils.TableSpec(file_path='cnts_daily.dopp_word.20150920', mapreduce_io_flags=['-fs', ',']),
            mr_utils.TableSpec(file_path='cnts_daily.dopp_word.20150921', mapreduce_io_flags=['-fs', ',']),
        ],
        output_tables=[
            mr_utils.TableSpec(file_path="dopp_word", table_name="cnts_aggr/dopp_word", mapreduce_io_flags=['-fs', ',']),
        ],
    )
