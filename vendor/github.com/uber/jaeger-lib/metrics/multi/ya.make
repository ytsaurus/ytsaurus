GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.4.1+incompatible)

SRCS(
    multi.go
)

GO_TEST_SRCS(multi_test.go)

END()

RECURSE(
    gotest
)
