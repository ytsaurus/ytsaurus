LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    components.cpp
    config.cpp
)

PEERDIR(
    yt/yt/library/signature/common
    yt/yt/library/signature/generation
    yt/yt/library/signature/validation
    yt/yt/core
    yt/yt/client
)

CHECK_DEPENDENT_DIRS(
    DENY
    PEERDIRS
    yt/yt/ytlib
    yt/yt/server
)

END()
