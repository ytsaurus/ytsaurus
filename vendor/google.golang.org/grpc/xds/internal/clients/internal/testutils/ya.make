GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.3)

SRCS(
    channel.go
    marshal_any.go
    restartable_listener.go
    wrappers.go
)

END()

RECURSE(
    e2e
    fakeserver
)
