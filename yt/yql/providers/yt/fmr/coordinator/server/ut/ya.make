UNITTEST()

SRCS(
    yql_yt_coordinator_server_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/client
    yt/yql/providers/yt/fmr/coordinator/impl
    yt/yql/providers/yt/fmr/coordinator/server
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
