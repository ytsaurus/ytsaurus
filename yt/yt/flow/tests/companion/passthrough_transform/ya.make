RECURSE(
    python
)

IF (NOT OPENSOURCE)
    RECURSE(
        go
    )
ENDIF()
