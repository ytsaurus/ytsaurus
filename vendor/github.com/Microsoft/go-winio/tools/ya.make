RECURSE(
    mkwinsyscall
)

IF (OS_LINUX)
    RECURSE(
        etw-provider-gen
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        etw-provider-gen
    )
ENDIF()

IF (OS_WINDOWS)
    RECURSE(
        etw-provider-gen
    )
ENDIF()
