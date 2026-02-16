GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.2)

SRCS(
    channelz.go
)

END()

RECURSE(
    grpc_channelz_v1
    internal
    service
)
