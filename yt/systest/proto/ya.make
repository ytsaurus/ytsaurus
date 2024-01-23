PROTO_LIBRARY()

SRCS(
    operation.proto
    run_spec.proto
    test_spec.proto
    table.proto
    validator.proto
)

PEERDIR(
    yt/yt_proto/yt/client
)

EXCLUDE_TAGS(
    GO_PROTO
    PY_PROTO
)

END()
