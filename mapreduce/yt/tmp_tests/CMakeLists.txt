OWNER(vartyukh exprmntr)

PYTEST()

TEST_SRCS(
    test_daily.py
    test_aggregate_new.py
    test_join.py
    test_qd_prepare.py
    test_monitoring.py
    test_collect.py
    test_aggregate.py
)

SIZE(FAT)
TIMEOUT(300)

DATA(
    arcadia/quality/mapreduce/tests/common
    arcadia_tests_data/quality/blender/wizards_clicks
    arcadia_tests_data/geo
    arcadia_tests_data/wizard/language
    arcadia_tests_data/wizard/synnorm
    sbr://98684160  # user_sessions.20150907.20k_sample
    sbr://98712097  # cnts_aggr_sample.dopp_word
    sbr://98700742  # random_pool.20151014_20151020.images_sample_10k
    sbr://98707673  # cnts_daily.dopp_word.20150920
    sbr://98708815  # cnts_daily.dopp_word.20150921
    sbr://98700621  # raw_clicks.20150907.doppel-word.10k_sample
)

DEPENDS(
    quality/mapreduce
    quality/blender/wizards_clicks
    mapreduce/yt/python
)

END()
