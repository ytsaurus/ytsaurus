LIBRARY()

SRCS(
    solomon_agent_client.cpp
)

PEERDIR(
    library/cpp/http/misc
    library/cpp/http/simple
    library/cpp/json
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/encode
    library/cpp/monlib/encode/spack
    library/cpp/monlib/metrics
    library/cpp/retry
    util
    yql/essentials/utils/log
    yql/essentials/providers/common/metrics
    yql/cfg/proto
    yql/tools/yqlworker/proto
)

END()
