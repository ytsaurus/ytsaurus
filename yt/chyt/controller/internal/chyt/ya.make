GO_LIBRARY()

SRCS(
    artifacts.go
    cluster_initializer.go
    configs.go
    controller.go
    fancy.go
    resources.go
    runtime.go
    speclet.go
)

GO_TEST_SRCS(resources_test.go)

END()

IF (NOT OPENSOURCE)
    RECURSE(gotest)
ENDIF()
