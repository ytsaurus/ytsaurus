GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    hkdf.go
    subtle.go
    x25519.go
)

GO_TEST_SRCS(hkdf_test.go)

GO_XTEST_SRCS(
    subtle_test.go
    # x25519_test.go
)

END()

RECURSE(
    gotest
    random
)
