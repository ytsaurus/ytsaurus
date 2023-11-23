GTEST(unittester-all)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    config_ut.cpp
)

SET(TEST_DATA_DIR ${ARCADIA_ROOT}/yt/yt/server/all/unittests/data)

RESOURCE(
    ${TEST_DATA_DIR}/controller-agent.yson /configs/controller-agent.yson
    ${TEST_DATA_DIR}/http-proxy.yson /configs/http-proxy.yson
    ${TEST_DATA_DIR}/map_reduce_spec.yson /configs/map_reduce_spec.yson
    ${TEST_DATA_DIR}/map_spec.yson /configs/map_spec.yson
    ${TEST_DATA_DIR}/master.yson /configs/master.yson
    ${TEST_DATA_DIR}/node.yson /configs/node.yson
    ${TEST_DATA_DIR}/reduce_spec.yson /configs/reduce_spec.yson
    ${TEST_DATA_DIR}/rpc-proxy.yson /configs/rpc-proxy.yson
    ${TEST_DATA_DIR}/scheduler.yson /configs/scheduler.yson
    ${TEST_DATA_DIR}/sort_spec.yson /configs/sort_spec.yson
    ${TEST_DATA_DIR}/vanilla_spec.yson /configs/vanilla_spec.yson
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/master
    yt/yt/server/cell_balancer
    yt/yt/server/clock_server
    yt/yt/server/controller_agent
    yt/yt/server/discovery_server
    yt/yt/server/exec
    yt/yt/server/http_proxy
    yt/yt/server/job_proxy
    yt/yt/server/log_tailer
    yt/yt/server/master
    yt/yt/server/master_cache
    yt/yt/server/node
    yt/yt/server/rpc_proxy
    yt/yt/server/scheduler
    yt/yt/server/timestamp_provider
    yt/yt/server/tools

    library/cpp/resource
)

SIZE(SMALL)

END()
