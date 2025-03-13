LIBRARY()

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h)
GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h)
GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_events.h)
GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_events.h)

SRCS(
    client.h
    codecs.h
    control_plane.h
    counters.h
    errors.h
    events_common.h
    executor.h
    read_events.h
    read_session.h
    retry_policy.h
    write_events.h
    write_session.h
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos

    library/cpp/streams/zstd
)

END()
