GO_TEST()

LICENSE(Apache-2.0)

VERSION(v1.71.0)

DATA(
    arcadia/vendor/google.golang.org/grpc/testdata
)

TEST_CWD(vendor/google.golang.org/grpc)

GO_XTEST_SRCS(ringhash_balancer_test.go)

END()
