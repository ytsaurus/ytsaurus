GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.3)

SRCS(
    pluginoption.go
)

END()

RECURSE(
    testutils
    tracing
)
