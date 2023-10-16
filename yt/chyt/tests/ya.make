IF (NOT GCC AND NOT OS_DARWIN)
    RECURSE(
        server
        dummy_logger
    )
ENDIF()
