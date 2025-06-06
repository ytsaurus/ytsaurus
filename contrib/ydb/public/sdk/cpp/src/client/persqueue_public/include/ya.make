LIBRARY()

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/src/client/persqueue_public/include/control_plane.h)
GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/src/client/persqueue_public/include/read_events.h)
GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/src/client/persqueue_public/include/write_events.h)
GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/src/client/persqueue_public/include/write_session.h)

SRCS(
    aliases.h
    client.h
    control_plane.h
    read_events.h
    read_session.h
    write_events.h
    write_session.h
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
)

END()
