PROTO_LIBRARY()

PROTO_NAMESPACE(yt/java/ytsaurus-testlib/src/main/proto)

SRCS(
    src/table_rows.proto
)

EXCLUDE_TAGS(CPP_PROTO GO_PROTO PY_PROTO PY3_PROTO)

END()
