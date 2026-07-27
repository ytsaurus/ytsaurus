GO_LIBRARY()

SRCS(
    artifacts.go
    cluster_initializer.go
    configs.go
    controller.go
    fancy.go
    odbc.go
    resources.go
    runtime.go
    speclet.go
)

GO_TEST_SRCS(
    controller_test.go
    resources_test.go
)

END()

IF (NOT OPENSOURCE)
    RECURSE(gotest)
ENDIF()
