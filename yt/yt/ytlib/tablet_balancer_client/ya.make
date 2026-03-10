LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(../../ya_check_dependencies.inc)

PROTO_NAMESPACE(yt)

SRCS(
    proto/tablet_balancer_service.proto

    balancing_request.cpp
)

PEERDIR(
    yt/yt/core
)

END()
