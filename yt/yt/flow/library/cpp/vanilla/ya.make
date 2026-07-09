LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    current_operation.cpp
    files.cpp
    spec.cpp
)

PEERDIR(
    yt/yt/flow/lib/native_client
    yt/yt/client
    yt/yt/client/cache
    yt/yt/library/auth
    yt/yt/core
    library/cpp/digest/md5
)

END()
