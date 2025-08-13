PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    contrib/ydb/public/api/protos/annotations
)

SRCS(
    validation_test.proto
)

CPP_PROTO_PLUGIN0(validation contrib/ydb/public/lib/validation)

EXCLUDE_TAGS(GO_PROTO)

END()
