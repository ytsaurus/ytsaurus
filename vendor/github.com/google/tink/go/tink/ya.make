GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    aead.go
    deterministic_aead.go
    hybrid_decrypt.go
    hybrid_encrypt.go
    mac.go
    signer.go
    streamingaead.go
    tink.go
    verifier.go
    version.go
)

END()
