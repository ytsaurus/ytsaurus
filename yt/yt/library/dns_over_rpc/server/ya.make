LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    dns_over_rpc_service.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/dns_over_rpc/client
)

END()
