LIBRARY()

SRCS(
    yql_yt_fmr_worker_server.cpp
    yql_yt_fmr_worker_server.h
)

PEERDIR(
    library/cpp/http/server
    yql/essentials/utils
    yql/essentials/utils/log
    yt/yql/providers/yt/fmr/worker/impl
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
