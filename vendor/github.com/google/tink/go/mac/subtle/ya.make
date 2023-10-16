GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    cmac.go
    hmac.go
)

GO_XTEST_SRCS(
    cmac_test.go
    hmac_test.go
)

END()

RECURSE(gotest)
