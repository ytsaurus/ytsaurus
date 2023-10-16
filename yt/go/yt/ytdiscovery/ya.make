GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    new_static.go
)

IF (NOT OPENSOURCE)
    SRCS(
        new_endpoint_set.go
    )
ENDIF()

END()
