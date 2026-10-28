GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.47.0)

IF (OS_WINDOWS)
    SRCS(
        install.go
        log.go
    )

    GO_XTEST_SRCS(log_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
