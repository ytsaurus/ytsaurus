GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.25.0)

SRCS(
    transform.go
)

GO_TEST_SRCS(transform_test.go)

GO_XTEST_SRCS(examples_test.go)

END()

RECURSE(
    # gotest
)
