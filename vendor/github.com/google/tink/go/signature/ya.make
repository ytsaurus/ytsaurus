GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    ecdsa_signer_key_manager.go
    ecdsa_verifier_key_manager.go
    ed25519_signer_key_manager.go
    ed25519_verifier_key_manager.go
    proto.go
    rsa.go
    rsassapkcs1_signer_key_manager.go
    rsassapkcs1_verifier_key_manager.go
    signature.go
    signature_key_templates.go
    signer_factory.go
    verifier_factory.go
)

GO_XTEST_SRCS(
    ecdsa_signer_key_manager_test.go
    ecdsa_verifier_key_manager_test.go
    ed25519_signer_key_manager_test.go
    ed25519_verifier_key_manager_test.go
    rsassapkcs1_signer_key_manager_test.go
    rsassapkcs1_verifier_key_manager_test.go
    signature_factory_test.go
    signature_key_templates_test.go
    signature_test.go
)

END()

RECURSE(
    gotest
    internal
    subtle
)
