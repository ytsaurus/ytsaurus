GO_LIBRARY()

LICENSE(BSD-2-Clause)

VERSION(v1.8.10)

SRCS(
    assert.go
)

GO_TEST_SRCS(assert_test.go)

END()

RECURSE(
    gotest
)
