GO_PROGRAM()

LICENSE(BSD-3-Clause)

VERSION(v0.33.0)

SRCS(
    mkmerge.go
)

GO_TEST_SRCS(mkmerge_test.go)

END()

RECURSE(
    gotest
)
