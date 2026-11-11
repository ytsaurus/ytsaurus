GTEST(unittester-cri)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    image_cache_ut.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/library/containers/cri
    yt/yt/library/process
)

SIZE(SMALL)

END()
