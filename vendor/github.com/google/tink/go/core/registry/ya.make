GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    key_manager.go
    kms_client.go
    private_key_manager.go
    registry.go
)

GO_XTEST_SRCS(registry_test.go)

END()

RECURSE(gotest)
