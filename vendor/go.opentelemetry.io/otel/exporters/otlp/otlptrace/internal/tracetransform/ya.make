GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.35.0)

SRCS(
    attribute.go
    instrumentation.go
    resource.go
    span.go
)

GO_TEST_SRCS(
    attribute_test.go
    instrumentation_test.go
    resource_test.go
    span_test.go
)

END()

RECURSE(
    gotest
)
