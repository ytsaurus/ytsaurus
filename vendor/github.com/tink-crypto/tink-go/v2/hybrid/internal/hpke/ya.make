GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.6.0)

SRCS(
    aead.go
    aes_gcm_aead.go
    chacha20poly1305_aead.go
    context.go
    decrypt.go
    encrypt.go
    hkdf_kdf.go
    hpke.go
    kdf.go
    kem.go
    nist_curves_kem.go
    primitive_factory.go
    x25519_kem.go
)

END()
