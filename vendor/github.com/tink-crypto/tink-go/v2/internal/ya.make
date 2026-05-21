GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.6.0)

SRCS(
    internal.go
)

END()

RECURSE(
    aead
    config
    ec
    internalapi
    internalregistry
    jwk
    keygenregistry
    legacykeymanager
    mac
    monitoringutil
    outputprefix
    prefixmap
    primitiveregistry
    primitiveset
    protoserialization
    registryconfig
    signature
    testing
    tinkerror
)
