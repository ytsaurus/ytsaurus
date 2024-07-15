GTEST(unittester-query-tracker-proxy)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)
INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SUBSCRIBER(g:yt)

SRCS(
    query_tracker_proxy_ut.cpp
)

PEERDIR(
    yt/yt/server/query_tracker
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
)

SIZE(MEDIUM)

END()
