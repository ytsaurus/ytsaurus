RECURSE(
    lib
    rpc
    rpc_shared
    native
    native_shared
)

IF (NOT OPENSOURCE)
    RECURSE(
        rpc_shared/py2
        rpc_shared/py3
        native_shared/py2
        native_shared/py3
    )
ENDIF()
