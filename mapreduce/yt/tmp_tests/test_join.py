# coding: utf-8

import yatest.common
import mr_utils


def test():
    data_path = '.'
    return mr_utils.mapreduce_test(
        yatest.common.binary_path("quality/blender/wizards_clicks/wizards_clicks"),
        args=[
            'join',
            '--src', 'random_pool/20151014_20151020/images_sample_10k',
            '--dst', 'random_pool/images_joined',
            '-c', 'cnts_aggr_sample',
            '-w', 'images',
            '-n', 'images',
            '-g', 'dopp_word',
            '--max_threads', '3',
            '--server', 'local',
            '--geodata', yatest.common.data_path('geo/geodata3.bin'),
            '--wiz_lang_dir', yatest.common.data_path('wizard/language'),
            '--wiz_syn_dir', yatest.common.data_path('wizard/synnorm'),
            '--ut',
        ],
        data_path=data_path,
        # counters table generate example: mapreduce-dev -read blender/wiz_clicks/cnts_aggr_new/20150411-20151011_50/dopp_query:[200000,] -fs , -count 10000
        input_tables=[
            mr_utils.TableSpec(file_path='cnts_aggr_sample.dopp_word', mapreduce_io_flags=['-fs', ',']),
            mr_utils.TableSpec(file_path='random_pool.20151014_20151020.images_sample_10k'),
        ],
        output_tables=[
            mr_utils.TableSpec(file_path="images_joined", table_name="random_pool/images_joined"),
        ],
    )
