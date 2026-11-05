PROTO_LIBRARY()

SRCS(
    inspector.proto
    function_registry.proto
    sql_library_resolve_result.proto
    worker.proto
)

PEERDIR(
    yql/essentials/public/issue/protos
    yql/tools/yqlworker/interface/proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
