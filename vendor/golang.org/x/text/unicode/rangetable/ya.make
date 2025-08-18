GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.26.0)

SRCS(
    merge.go
    rangetable.go
    tables15.0.0.go
)

GO_TEST_SRCS(
    merge_test.go
    rangetable_test.go
)

END()

RECURSE(
    gotest
)
