RECURSE(
    public
)

IF (NOT OPENSOURCE)
    RECURSE(
        flow
        private
    )
ENDIF()
