LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    components.cpp
    config.cpp
)

PEERDIR(
    yt/yt/server/lib/signature/common
    yt/yt/server/lib/signature/generation
    yt/yt/server/lib/signature/validation
    yt/yt/ytlib
    yt/yt/core
    yt/yt/client
)

END()
