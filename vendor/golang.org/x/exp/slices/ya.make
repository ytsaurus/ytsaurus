GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.0.0-20240613232115-7f521ea00fb8)

SRCS(
    cmp.go
    slices.go
    sort.go
    zsortanyfunc.go
    zsortordered.go
)

GO_TEST_SRCS(
    slices_test.go
    sort_benchmark_test.go
    sort_test.go
)

END()

RECURSE(
    gotest
)
