LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    dns_over_rpc_resolver.cpp
    helpers.cpp

    proto/dns_over_rpc_service.proto
)

PEERDIR(
    yt/yt/core
)

END()
