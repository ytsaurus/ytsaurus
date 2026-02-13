PROGRAM(ytserver-all)

INCLUDE(${ARCADIA_ROOT}/yt/yt/ya_check_dependencies.inc)

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
    yt/yt/server/chaos_cache
    yt/yt/server/clock_server
    yt/yt/server/controller_agent
    yt/yt/server/cypress_proxy
    yt/yt/server/discovery_server
    yt/yt/server/exec
    yt/yt/server/http_proxy
    yt/yt/server/job_proxy
    yt/yt/server/kafka_proxy
    yt/yt/server/log_tailer
    yt/yt/server/master
    yt/yt/server/master_cache
    yt/yt/server/node
    yt/yt/server/queue_agent
    yt/yt/server/query_tracker
    yt/yt/server/rpc_proxy
    yt/yt/server/scheduler
    yt/yt/server/sequoia_reconstructor
    yt/yt/server/tablet_balancer
    yt/yt/server/tcp_proxy
    yt/yt/server/timestamp_provider
    yt/yt/server/tools
    yt/yt/server/replicated_table_tracker
    yt/yt/server/multidaemon
    yt/yt/server/offshore_data_gateway

    yt/yt/library/oom
)

IF (YT_ROPSAN_ENABLE_ACCESS_CHECK)
    CXXFLAGS(-DYT_ROPSAN_ENABLE_ACCESS_CHECK)
ENDIF()

IF (YT_ROPSAN_ENABLE_SERIALIZATION_CHECK)
    CXXFLAGS(-DYT_ROPSAN_ENABLE_SERIALIZATION_CHECK)
ENDIF()

IF (YT_ROPSAN_ENABLE_LEAK_DETECTION)
    CXXFLAGS(-DYT_ROPSAN_ENABLE_LEAK_DETECTION)
ENDIF()

IF (YT_ROPSAN_ENABLE_PTR_TAGGING)
    CXXFLAGS(-DYT_ROPSAN_ENABLE_PTR_TAGGING)
ENDIF()

END()

RECURSE_FOR_TESTS(unittests)
