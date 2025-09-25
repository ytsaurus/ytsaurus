GO_LIBRARY()

SRCS(
    client.go
)

IF (OPENSOURCE)
    SRCS(
        client_external.go
    )
ELSE()
    SRCS(
        client_internal.go
    )
ENDIF()

END()
