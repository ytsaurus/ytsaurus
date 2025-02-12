UNITTEST_FOR(contrib/ydb/library/yql/dq/actors/compute)

SRCS(
    dq_compute_actor_ut.cpp
    dq_compute_actor_async_input_helper_ut.cpp
    dq_compute_issues_buffer_ut.cpp
    dq_source_watermark_tracker_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/yql/public/ydb_issue
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/actors/wilson
    contrib/ydb/library/actors/testlib
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    yql/essentials/minikql/comp_nodes/no_llvm
)

YQL_LAST_ABI_VERSION()

END()
