GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(dynamic.go)

GO_XTEST_SRCS(dynamic_test.go)

END()

RECURSE(gotest)
