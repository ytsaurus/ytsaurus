GTEST(unittester-yt-orm-library)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    computed_fields_filter_ut.cpp
    expression_evaluator_ut.cpp
    filter_introspection_ut.cpp
    filter_matcher_ut.cpp
    query_optimizer_ut.cpp
    split_filter_ut.cpp
    type_inference_ut.cpp
    unaggregated_column_detector_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/orm/library/query

    yt/yt/core/test_framework
)

END()
