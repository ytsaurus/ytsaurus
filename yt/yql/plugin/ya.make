LIBRARY()

SRCS(
    plugin.cpp
    config.cpp
    udf_meta.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/ytlib
)

END()

RECURSE(
    bridge
    dynamic
    native
    process
    qtworker
)
