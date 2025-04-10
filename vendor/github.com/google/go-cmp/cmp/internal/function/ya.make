GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.7.0)

SRCS(
    func.go
)

GO_TEST_SRCS(func_test.go)

END()

RECURSE(
    gotest
)
