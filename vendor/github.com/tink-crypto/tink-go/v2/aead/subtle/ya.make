GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.6.0)

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

END()
