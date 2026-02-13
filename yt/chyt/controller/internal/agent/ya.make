GO_LIBRARY()

SRCS(
    agent.go
    config.go
    crashed_job_monitor.go
    health_state.go
    helpers.go
    job_checker.go
    operation_collector.go
    tracker.go
)

GO_TEST_SRCS(tracker_test.go)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        gotest
    )
ENDIF()
