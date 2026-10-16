GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.13.4-0)

SRCS(
    dgxa100.go
)

GO_TEST_SRCS(dgxa100_test.go)

END()

RECURSE(
    gotest
)
