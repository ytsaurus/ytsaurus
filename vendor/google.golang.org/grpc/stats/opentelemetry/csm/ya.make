GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.3)

SRCS(
    observability.go
    pluginoption.go
)

GO_TEST_SRCS(
    observability_test.go
    pluginoption_test.go
)

END()

RECURSE(
    gotest
)
