GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    compiler.go
    content.go
    doc.go
    draft.go
    errors.go
    extension.go
    format.go
    loader.go
    output.go
    resource.go
    schema.go
)

GO_TEST_SRCS(
    # format_test.go
    internal_test.go
)

GO_XTEST_SRCS(
    example_extension_test.go
    example_test.go
    extension_test.go
    # schema_test.go
)

END()

RECURSE(
    gotest
    httploader
)
