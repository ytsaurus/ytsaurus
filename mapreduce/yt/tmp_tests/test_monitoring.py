# coding: utf-8

import yatest.common
import mr_utils


def test():
    data_path = '.'
    return mr_utils.mapreduce_test(
        yatest.common.binary_path("quality/blender/wizards_clicks/wizards_clicks"),
        args=[
            'monitoring',
            '--src', 'cnts_aggr_sample/dopp_word',
            '--dst', 'cnts_aggr_sample/dopp_word/monitoring',
            '--server', 'local',
        ],
        data_path=data_path,
        input_tables=[
            mr_utils.TableSpec(file_path='cnts_aggr_sample.dopp_word', mapreduce_io_flags=['-fs', ',']),
        ],
        output_tables=[
            mr_utils.TableSpec(file_path="cnts_aggr_sample.dopp_word.monitoring"),
        ],
    )
