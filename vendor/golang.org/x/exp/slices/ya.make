GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.0.0-20250813145105-42675adae3e6)

SRCS(
    slices.go
    sort.go
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
