SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        apis.go
        emit.go
        hooks.go
        testsupport.go
    )
ENDIF()
END()
