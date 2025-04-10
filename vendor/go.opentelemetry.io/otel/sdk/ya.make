GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.35.0)

SRCS(
    version.go
)

GO_XTEST_SRCS(version_test.go)

END()

RECURSE(
    gotest
    instrumentation
    internal
    resource
    trace
)
