GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(walk.go)

GO_XTEST_SRCS(walk_test.go)

IF (OPENSOURCE)
    GO_XTEST_SRCS(main_test.go)
ENDIF()

END()

RECURSE(gotest)
