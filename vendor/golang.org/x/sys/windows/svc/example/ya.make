GO_PROGRAM()

LICENSE(BSD-3-Clause)

VERSION(v0.47.0)

IF (OS_WINDOWS)
    SRCS(
        beep.go
        install.go
        main.go
        manage.go
        service.go
    )
ENDIF()

END()
