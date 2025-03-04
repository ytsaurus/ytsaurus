GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.0.0-20240613232115-7f521ea00fb8)

SRCS(
    constraints.go
)

GO_TEST_SRCS(constraints_test.go)

END()

RECURSE(
    gotest
)
