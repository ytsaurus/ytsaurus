GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    page_cache_ut.cpp
)

PEERDIR(
    yt/yt/server/lib/nbd/chunk
    yt/yt/core/test_framework
)

END()
