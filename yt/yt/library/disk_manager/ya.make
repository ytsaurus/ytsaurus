LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/rpc/grpc
)

SRCS(
    config.cpp
    disk_info_provider.cpp
    hotswap_manager.cpp
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ELSE()
    SRCS(
        disk_manager_proxy_stub.cpp
    )
ENDIF()

END()

IF (NOT OPENSOURCE)
RECURSE_FOR_TESTS(
    unittests
)
ENDIF()
