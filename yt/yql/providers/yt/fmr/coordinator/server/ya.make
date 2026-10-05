YQL_LIBRARY()

SRCS(
    yql_yt_coordinator_server.cpp
)

PEERDIR(
    library/cpp/http/server
    yql/essentials/utils
    yql/essentials/utils/log
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/fmr/coordinator/interface/proto_helpers
    yt/yql/providers/yt/fmr/proto
    yt/yql/providers/yt/fmr/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)
