PROTO_LIBRARY()

LICENSE(
    BSD-3-Clause AND
    Protobuf-License
)

PROTO_NAMESPACE(
    yt/yt/contrib/gogoproto
)

ONLY_TAGS(
    CPP_PROTO
)

SRCS(
    github.com/gogo/protobuf/gogoproto/gogo.proto
)

END()
