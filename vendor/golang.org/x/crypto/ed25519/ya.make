GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.31.0)

SRCS(
    ed25519.go
)

GO_XTEST_SRCS(ed25519_test.go)

END()

RECURSE(
    gotest
)
