GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.2)

SRCS(
    admin.go
)

GO_XTEST_SRCS(admin_test.go)

END()

RECURSE(
    gotest
    test
)
