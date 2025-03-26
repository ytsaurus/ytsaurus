GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.32.0)

SRCS(
    block.go
    cipher.go
    const.go
)

GO_TEST_SRCS(blowfish_test.go)

END()

RECURSE(
    gotest
)
