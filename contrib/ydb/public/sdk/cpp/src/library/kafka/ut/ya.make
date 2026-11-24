UNITTEST_FOR(contrib/ydb/public/sdk/cpp/src/library/kafka)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    kafka_messages_int_ut.cpp
    kafka_records_ut.cpp
)

END()
