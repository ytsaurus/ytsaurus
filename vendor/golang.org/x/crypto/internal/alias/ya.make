GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    alias.go
)

GO_TEST_SRCS(alias_test.go)

END()

RECURSE(
    gotest
)
