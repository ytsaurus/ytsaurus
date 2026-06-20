GTEST(unittester-experiments-logslice)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    logslice_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tools/logslice/lib
    yt/yt/core
    yt/yt/core/test_framework
)

SIZE(SMALL)

END()

RECURSE_FOR_TESTS(
    py
)
