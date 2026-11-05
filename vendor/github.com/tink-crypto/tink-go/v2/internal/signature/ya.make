GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.6.0)

SRCS(
    rsa.go
    rsassapkcs1_signer.go
    rsassapkcs1_verifier.go
    rsassapss_signer.go
    rsassapss_verifier.go
    signature.go
)

END()

RECURSE(
    ecdsa
    mldsa
    slhdsa
)
