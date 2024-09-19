RECURSE(
    test
    writer
)

IF (OS_LINUX)
    RECURSE(
        syslog
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        syslog
    )
ENDIF()
