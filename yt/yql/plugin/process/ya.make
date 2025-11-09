LIBRARY()

SRCS(
    config.cpp
    plugin.cpp
    plugin_service.cpp
    process.cpp
    program.cpp
    public.cpp
)

PEERDIR(
    library/cpp/retry
    yt/yt/ytlib
    yt/cpp/mapreduce/common
    yt/yql/plugin/bridge
    yt/yql/plugin
)

END()
