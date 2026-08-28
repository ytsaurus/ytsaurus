GO_LIBRARY()

SRCS(snappy.go)

GO_TEST_SRCS(snappy_test.go)

END()

RECURSE(
    gotest
)
