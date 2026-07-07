LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    expression_evaluator.cpp
    filter_matcher.cpp
)

PEERDIR(
    yt/yt/orm/library/query

    yt/yt/library/query/engine
    yt/yt/library/query/engine_api

    yt/yt/core
    yt/yt/client
)

END()
