GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    internal.go
    rsa.go
    rsassapkcs1_signer.go
    rsassapkcs1_verifier.go
)

GO_XTEST_SRCS(
    rsa_test.go
    rsassapkcs1_signer_verifier_test.go
)

END()

RECURSE(gotest)
