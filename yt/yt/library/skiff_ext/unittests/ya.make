GTEST(unittester-skiff-ext)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    schema_match_ut.cpp
    ut_helpers.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/client
    yt/yt/library/skiff_ext
)

SIZE(SMALL)

END()
