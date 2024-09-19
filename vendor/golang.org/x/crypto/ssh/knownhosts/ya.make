GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    knownhosts.go
)

GO_TEST_SRCS(knownhosts_test.go)

END()

RECURSE(
    gotest
)
