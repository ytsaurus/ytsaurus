GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.78.0)

SRCS(
    watch_service.go
    xds_dependency_manager.go
)

GO_XTEST_SRCS(
    # xds_dependency_manager_test.go
)

END()

RECURSE(
    gotest
)
