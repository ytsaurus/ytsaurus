GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.36.0)

SRCS(
    alias.go
)

GO_TEST_SRCS(alias_test.go)

END()

RECURSE(
    gotest
)
