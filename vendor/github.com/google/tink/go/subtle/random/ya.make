GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.7.0)

SRCS(
    random.go
)

GO_XTEST_SRCS(random_test.go)

END()

RECURSE(
    gotest
)
