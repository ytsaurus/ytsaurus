UNITTEST_FOR(yt/yql/providers/dq/global_worker_manager)

NO_BUILD_IF(OS_WINDOWS)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/providers/dq/actors
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/computation/llvm16
    yql/essentials/providers/common/comp_nodes
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    yql/essentials/utils/log
    yt/yql/providers/dq/actors/yt
    yt/yql/providers/dq/runtime
    yt/yql/providers/dq/testlib
)

SRCS(
    coordination_helper_ut.cpp
    global_worker_manager_ut.cpp
    interconnect_ut.cpp
    service_node_pinger_ut.cpp
    workers_storage_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/supp/ubsan_supp.inc)

YQL_LAST_ABI_VERSION()

END()
