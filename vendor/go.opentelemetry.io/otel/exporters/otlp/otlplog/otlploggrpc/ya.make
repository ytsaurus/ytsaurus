GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

VERSION(v0.14.0)

SRCS(
    client.go
    config.go
    doc.go
    exporter.go
    version.go
)

GO_TEST_SRCS(
    client_test.go
    config_test.go
    exporter_test.go
    version_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
    internal
)
