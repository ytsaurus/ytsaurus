LIBRARY()

SRCS(
    provider_load.cpp
    ytflowrun_lib.cpp
)

PEERDIR(
    yt/yql/tools/ytrun/lib

    yt/yql/providers/ytflow/gateway
    yt/yql/providers/ytflow/provider
)

YQL_LAST_ABI_VERSION()

END()
