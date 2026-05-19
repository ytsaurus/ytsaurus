LIBRARY()

SRCS(
    yql_yt_vanilla_http_mon.cpp
)

PEERDIR(
    library/cpp/http/misc
    library/cpp/http/server
    library/cpp/json
    yt/yql/providers/yt/fmr/vanilla/peer_tracker
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
