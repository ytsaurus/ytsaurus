GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.47.0)

IF (OS_WINDOWS)
    SRCS(
        log.go
        service.go
    )
ENDIF()

END()
