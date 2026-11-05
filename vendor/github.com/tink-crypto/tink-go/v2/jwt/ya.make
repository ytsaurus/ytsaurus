GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.6.0)

SRCS(
    jwk_converter.go
    jwt.go
    jwt_config.go
    jwt_encoding.go
    jwt_full_mac.go
    jwt_full_signer_verifier.go
    jwt_key_templates.go
    jwt_mac.go
    jwt_mac_factory.go
    jwt_mac_kid.go
    jwt_signer.go
    jwt_signer_factory.go
    jwt_signer_kid.go
    jwt_validator.go
    jwt_verifier.go
    jwt_verifier_factory.go
    jwt_verifier_kid.go
    raw_jwt.go
    verified_jwt.go
)

END()

RECURSE(
    jwtecdsa
    jwthmac
    jwtrsassapkcs1
    jwtrsassapss
)
