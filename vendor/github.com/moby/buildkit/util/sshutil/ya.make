GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.12.2)

SRCS(
    keyscan.go
    transport_validation.go
)

GO_TEST_SRCS(transport_validation_test.go)

END()

RECURSE(
    gotest
)
