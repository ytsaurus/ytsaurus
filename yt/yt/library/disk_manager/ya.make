LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/rpc/grpc
)

SRCS(
    config.cpp
    disk_info_provider.cpp
    disk_manager_proxy_stub.cpp
    hotswap_manager.cpp
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
