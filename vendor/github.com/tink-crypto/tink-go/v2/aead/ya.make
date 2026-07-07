GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.6.0)

SRCS(
    aead.go
    aead_factory.go
    aead_key_templates.go
    kms_envelope_aead.go
    kms_envelope_aead_key_manager.go
)

END()

RECURSE(
    aesctrhmac
    aesgcm
    aesgcmsiv
    chacha20poly1305
    internal
    subtle
    xaesgcm
    xchacha20poly1305
)
