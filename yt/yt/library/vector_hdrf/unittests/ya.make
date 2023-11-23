GTEST(unittester-fair-share-update)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    fair_share_update_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/vector_hdrf
    yt/yt/core/test_framework
)

END()
