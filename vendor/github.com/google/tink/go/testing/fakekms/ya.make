GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.7.0)

SRCS(
    fakekms.go
)

GO_XTEST_SRCS(fakekms_test.go)

END()

RECURSE(
    gotest
)
