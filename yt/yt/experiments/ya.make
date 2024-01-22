RECURSE(
    public
)

IF (NOT OPENSOURCE)
    RECURSE(
        private
    )
ENDIF()
