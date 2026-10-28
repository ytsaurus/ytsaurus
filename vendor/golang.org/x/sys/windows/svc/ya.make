GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.47.0)

IF (OS_WINDOWS)
    SRCS(
        security.go
        service.go
    )

    GO_XTEST_SRCS(svc_test.go)
ENDIF()

END()

RECURSE(
    gotest
)

IF (OS_WINDOWS)
    RECURSE(
        debug
        mgr
        example
        eventlog
    )
ENDIF()
