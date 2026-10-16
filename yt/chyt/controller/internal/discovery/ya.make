GO_LIBRARY()

SRCS(
    client.go
)

IF (NOT OPENSOURCE)
    SRCS(discovery_client.go)
ELSE()
    SRCS(static_discovery_client.go)
ENDIF()

END()
