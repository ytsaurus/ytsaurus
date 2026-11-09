GTEST(unittester-library-query-base)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    scalar_preparation_ut.cpp
    scalar_semantics_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/library/query/base
)

SIZE(SMALL)

END()
