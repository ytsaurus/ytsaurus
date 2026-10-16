GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.6.0)

SRCS(
    hkdf.go
    subtle.go
    x25519.go
)

END()

RECURSE(
    random
)
