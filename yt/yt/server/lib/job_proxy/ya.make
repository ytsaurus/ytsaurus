LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/dns_over_rpc/client
    yt/yt/server/lib/misc
    yt/yt/server/lib/rpc_proxy
)

END()
