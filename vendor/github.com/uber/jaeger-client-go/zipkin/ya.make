GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    propagation.go
)

GO_TEST_SRCS(propagation_test.go)

END()

RECURSE(gotest)
