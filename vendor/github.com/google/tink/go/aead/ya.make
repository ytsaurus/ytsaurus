GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    aead.go
    aead_factory.go
    aead_key_templates.go
    aes_ctr_hmac_aead_key_manager.go
    aes_gcm_key_manager.go
    aes_gcm_siv_key_manager.go
    chacha20poly1305_key_manager.go
    kms_envelope_aead.go
    kms_envelope_aead_key_manager.go
    xchacha20poly1305_key_manager.go
)

GO_XTEST_SRCS(
    aead_factory_test.go
    aead_key_templates_test.go
    aead_test.go
    aes_ctr_hmac_aead_key_manager_test.go
    aes_gcm_key_manager_test.go
    aes_gcm_siv_key_manager_test.go
    chacha20poly1305_key_manager_test.go
    kms_envelope_aead_test.go
    xchacha20poly1305_key_manager_test.go
)

END()

RECURSE(
    gotest
    subtle
)
