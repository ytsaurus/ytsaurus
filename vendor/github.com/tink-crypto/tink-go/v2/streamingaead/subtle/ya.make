GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.6.0)

SRCS(
    aes_ctr_hmac.go
    aes_gcm_hkdf.go
    subtle.go
)

END()

RECURSE(
    noncebased
)
