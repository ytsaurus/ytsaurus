GO_LIBRARY()

LICENSE(BSD-2-Clause)

SRCS(
    assert.go
)

GO_TEST_SRCS(assert_test.go)

END()

RECURSE(
    gotest
)
