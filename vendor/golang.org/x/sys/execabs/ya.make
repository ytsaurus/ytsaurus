GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.47.0)

SRCS(
    execabs.go
    execabs_go119.go
)

GO_TEST_SRCS(execabs_test.go)

END()

RECURSE(
    gotest
)
