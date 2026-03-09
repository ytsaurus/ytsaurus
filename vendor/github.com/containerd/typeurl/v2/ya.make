GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.2.3)

SRCS(
    doc.go
    types.go
    types_gogo.go
)

GO_TEST_SRCS(
    marshal_test.go
    types_test.go
)

END()

RECURSE(
    gotest
)
