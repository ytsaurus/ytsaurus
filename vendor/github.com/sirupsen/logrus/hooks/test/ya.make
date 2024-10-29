GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.9.3)

SRCS(
    test.go
)

GO_TEST_SRCS(test_test.go)

END()

RECURSE(
    gotest
)
