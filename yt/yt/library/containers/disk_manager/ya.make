LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
)

SRCS(
    config.cpp
    disk_info_provider.cpp
    disk_manager_proxy.cpp
)

IF (NOT OPENSOURCE)
    SRCS(
        GLOBAL disk_manager_proxy_impl.cpp
    )

    PEERDIR(
        infra/diskmanager/proto
    )
ENDIF()

END()
