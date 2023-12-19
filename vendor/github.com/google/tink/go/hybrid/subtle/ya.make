GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    ecies_aead_hkdf_dem_helper.go
    ecies_aead_hkdf_hybrid_decrypt.go
    ecies_aead_hkdf_hybrid_encrypt.go
    ecies_hkdf_recipient_kem.go
    ecies_hkdf_sender_kem.go
    elliptic_curves.go
    public_key.go
    subtle.go
)

GO_XTEST_SRCS(
    elliptic_curves_test.go
    public_key_test.go
    subtle_test.go
)

END()

RECURSE(gotest)
