GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(internal_registry.go)

GO_XTEST_SRCS(internal_registry_test.go)

END()

RECURSE(gotest)
