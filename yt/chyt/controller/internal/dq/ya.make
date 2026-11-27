GO_LIBRARY()

SRCS(
    api.go
    artifacts.go
    cluster_initializer.go
    commands.go
    config.go
    controller.go
    discovery.go
    runtime.go
    speclet.go
)

GO_TEST_SRCS(
    config_test.go
    controller_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
)
