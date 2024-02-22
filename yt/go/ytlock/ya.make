GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    helpers.go
    lock.go
    locker.go
)

GO_TEST_SRCS(lock_test.go)

IF (NOT OPENSOURCE)
    GO_TEST_SRCS(locker_test.go)
ENDIF()

END()

RECURSE(
    gotest
)

IF (NOT OPENSOURCE)
    RECURSE(integration)
ENDIF()
