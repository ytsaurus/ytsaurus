LIBRARY()

SRCS(
    yql_yt_mixed.cpp
)

PEERDIR(
    yql/essentials/utils/log
    yt/yql/providers/yt/provider
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/gateway/lib
    yt/yql/providers/yt/common
    yql/essentials/providers/common/provider

    library/cpp/threading/future
    library/cpp/yson/node
)

YQL_LAST_ABI_VERSION()

END()
