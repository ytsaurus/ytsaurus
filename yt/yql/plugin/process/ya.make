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
    yt/yql/plugin   
    yt/yql/providers/yt/lib/yt_download
)

YQL_LAST_ABI_VERSION()

END()
