GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(walk.go)

GO_XTEST_SRCS(walk_test.go)

END()

IF (NOT OPENSOURCE)
    RECURSE(gotest)
ENDIF()
