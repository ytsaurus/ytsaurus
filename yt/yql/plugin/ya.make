LIBRARY()

SRCS(
    plugin.cpp
    config.cpp
)

PEERDIR(
    yt/yt/core
)

END()

RECURSE(
    bridge
    dynamic
    native
    process
)
