GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    aead.go
    aes_gcm_insecure_iv.go
    chacha20poly1305_insecure_nonce.go
)

GO_XTEST_SRCS(
    aead_test.go
    aes_gcm_insecure_iv_test.go
    chacha20poly1305_insecure_nonce_test.go
    chacha20poly1305_insecure_nonce_vectors_test.go
)

END()

RECURSE(gotest)
