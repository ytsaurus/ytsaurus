PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    portion_info.proto
    index.proto
)

PEERDIR(
    contrib/ydb/library/formats/arrow/protos
    contrib/ydb/core/tx/columnshard/common/protos

)

END()
