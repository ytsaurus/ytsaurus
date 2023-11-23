GTEST(unittester-http-proxy)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    helpers_ut.cpp
    http_proxy_ut.cpp
)

ADDINCL(
    contrib/libs/sparsehash/src
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/http_proxy
)

END()
