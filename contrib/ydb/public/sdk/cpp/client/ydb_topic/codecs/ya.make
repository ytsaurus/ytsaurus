LIBRARY()

SRCS(
    codecs.h
    codecs.cpp
)

PEERDIR(
    library/cpp/streams/zstd
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
)

PROVIDES(topic_codecs)

END()
