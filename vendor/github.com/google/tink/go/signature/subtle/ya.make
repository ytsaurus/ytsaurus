GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    ecdsa.go
    ecdsa_signer.go
    ecdsa_verifier.go
    ed25519_signer.go
    ed25519_verifier.go
    encoding.go
    subtle.go
)

GO_XTEST_SRCS(
    ecdsa_signer_verifier_test.go
    ecdsa_test.go
    ed25519_signer_verifier_test.go
    subtle_test.go
)

END()

RECURSE(gotest)
