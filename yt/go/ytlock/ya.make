GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    helpers.go
    lock.go
)

GO_TEST_SRCS(lock_test.go)

END()

RECURSE(
    gotest
)

IF (NOT OPENSOURCE)
    RECURSE(integration)
ENDIF()
