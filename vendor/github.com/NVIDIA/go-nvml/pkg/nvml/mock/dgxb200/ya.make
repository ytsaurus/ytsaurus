GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.13.4-0)

ALL_GO_SRCS()

GO_TEST_SRCS(dgxb200_test.go)

END()

RECURSE(
    gotest
)
