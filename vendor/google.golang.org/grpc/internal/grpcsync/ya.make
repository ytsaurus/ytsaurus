GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.69.4)

SRCS(
    callback_serializer.go
    event.go
    oncefunc.go
    pubsub.go
)

GO_TEST_SRCS(
    callback_serializer_test.go
    event_test.go
    oncefunc_test.go
    pubsub_test.go
)

END()

RECURSE(
    gotest
)
