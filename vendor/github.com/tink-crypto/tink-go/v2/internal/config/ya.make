GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.6.0)

SRCS(
    config.go
)

END()

RECURSE(
    aeadconfig
    daeadconfig
    hybridconfig
    jwtmacconfig
    jwtsignatureconfig
    keyderivationconfig
    macconfig
    prfconfig
    signatureconfig
    streamingaeadconfig
)
