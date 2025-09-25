RECURSE(
    external
)

IF (NOT OPENSOURCE)
    RECURSE(
        internal
    )
ENDIF()
