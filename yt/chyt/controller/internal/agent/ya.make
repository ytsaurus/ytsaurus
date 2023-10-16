GO_LIBRARY()

SRCS(
    agent.go
    config.go
    health_state.go
    helpers.go
    operation_collector.go
    tracker.go
)

GO_TEST_SRCS(tracker_test.go)

END()

IF (NOT OPENSOURCE)
    RECURSE(gotest)
ENDIF()
