RECURSE(
    etw
)

IF (OS_WINDOWS)
    RECURSE(
        security
        fs
        guid
        etwlogrus
        process
        bindfilter
    )
ENDIF()
