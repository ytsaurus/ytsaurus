PROGRAM(ytserver-all)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    yt/yt/server/all/main.cpp
)

PEERDIR(
    yt/yt/library/query/engine
    yt/yt/library/query/row_comparer

    yt/yt/server/cell_balancer
    yt/yt/server/clock_server
    yt/yt/server/controller_agent
    yt/yt/server/cypress_proxy
    yt/yt/server/discovery_server
    yt/yt/server/exec
    yt/yt/server/http_proxy
    yt/yt/server/job_proxy
    yt/yt/server/log_tailer
    yt/yt/server/master
    yt/yt/server/master_cache
    yt/yt/server/node
    yt/yt/server/queue_agent
    yt/yt/server/query_tracker
    yt/yt/server/rpc_proxy
    yt/yt/server/scheduler
    yt/yt/server/tablet_balancer
    yt/yt/server/tcp_proxy
    yt/yt/server/timestamp_provider
    yt/yt/server/tools

    yt/yt/library/oom
)

END()

RECURSE_FOR_TESTS(unittests)
