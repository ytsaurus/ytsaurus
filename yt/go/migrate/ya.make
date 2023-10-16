GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    create.go
    migrate.go
    ttl.go
)

END()

IF (NOT OPENSOURCE)
    RECURSE(integration)
ENDIF()
