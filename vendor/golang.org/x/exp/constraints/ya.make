GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.0.0-20240707233637-46b078467d37)

SRCS(
    constraints.go
)

GO_TEST_SRCS(constraints_test.go)

END()

RECURSE(
    gotest
)
