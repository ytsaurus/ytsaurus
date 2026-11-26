GO_LIBRARY()

IF (OPENSOURCE)
    SRCS(
        embed_external.go
    )

    GO_EMBED_PATTERN(
        external_swagger.json
    )
ELSE()
    SRCS(
        embed_internal.go
    )

    GO_EMBED_PATTERN(
        internal_swagger.json
    )
ENDIF()

END()
