UNITTEST_FOR(contrib/ydb/library/yql/dq/runtime/streaming)

SRCS(
    dq_source_watermark_tracker_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/yql/dq/runtime/streaming
)

END()
