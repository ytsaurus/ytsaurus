GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    aes_ctr.go
    aes_gcm.go
    aes_gcm_siv.go
    chacha20poly1305.go
    encrypt_then_authenticate.go
    ind_cpa.go
    polyval.go
    subtle.go
    xchacha20poly1305.go
)

GO_XTEST_SRCS(
    aes_ctr_test.go
    aes_gcm_siv_test.go
    aes_gcm_test.go
    chacha20poly1305_test.go
    chacha20poly1305_vectors_test.go
    encrypt_then_authenticate_test.go
    polyval_test.go
    subtle_test.go
    # xchacha20poly1305_test.go
    # xchacha20poly1305_vectors_test.go
)

END()

RECURSE(gotest)
