LIBRARY()

SRCS(
    plugin.cpp
    plugin_service.cpp
    config.cpp
    public.cpp
)

PEERDIR(
    library/cpp/retry
    yt/yt/ytlib
    yt/cpp/mapreduce/common
    yt/yql/plugin/bridge
)

END()
