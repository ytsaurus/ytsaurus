GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    aes_cmac.go
    hkdf.go
    hmac.go
    subtle.go
)

GO_XTEST_SRCS(
    aes_cmac_test.go
    hkdf_test.go
    hmac_test.go
    subtle_test.go
)

END()

RECURSE(gotest)
