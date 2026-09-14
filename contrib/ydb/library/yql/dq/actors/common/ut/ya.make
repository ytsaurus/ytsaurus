UNITTEST_FOR(contrib/ydb/library/yql/dq/actors/common)

SRCS(
    retry_events_queue_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/library/yql/dq/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
