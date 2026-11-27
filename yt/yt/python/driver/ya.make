RECURSE(
    lib
    rpc
    rpc_shared
)

IF (NOT OPENSOURCE)
    RECURSE(
        rpc_shared/py2
        rpc_shared/py3
    )
ENDIF()

IF (NOT OS_WINDOWS)
    RECURSE(
        native
        native_shared
    )

    IF (NOT OPENSOURCE)
        RECURSE(
            native_shared/py2
            native_shared/py3
        )
    ENDIF()
ENDIF()
