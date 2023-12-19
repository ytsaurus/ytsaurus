LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    access_checker.cpp
    api_service.cpp
    config.cpp
    format_row_stream.cpp
    profilers.cpp
    helpers.cpp
    proxy_coordinator.cpp
    security_manager.cpp
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/library/auth_server

    yt/yt/client
    yt/yt/client/arrow

    # TODO(max42): eliminate.
    yt/yt/server/lib/misc

    yt/yt/library/error_skeleton

    yt/yt/server/lib/transaction_server

    yt/yt/core
)

END()
