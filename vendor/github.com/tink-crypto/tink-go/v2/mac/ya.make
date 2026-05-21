GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.6.0)

SRCS(
    mac.go
    mac_factory.go
    mac_key_templates.go
)

END()

RECURSE(
    aescmac
    hmac
    internal
    subtle
)
