GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    types.go
)

GO_TEST_SRCS(
    marshal_test.go
    types_test.go
)

END()

RECURSE(
    gotest
)
