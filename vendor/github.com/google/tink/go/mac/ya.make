GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    aes_cmac_key_manager.go
    hmac_key_manager.go
    mac.go
    mac_factory.go
    mac_key_templates.go
)

GO_XTEST_SRCS(
    aes_cmac_key_manager_test.go
    hmac_key_manager_test.go
    mac_factory_test.go
    mac_key_templates_test.go
    mac_test.go
)

END()

RECURSE(
    gotest
    subtle
)
