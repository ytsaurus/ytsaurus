

IF (YT_TEAMCITY)
    # temporary (TC disk overload)
    RECURSE(
        trunk_native_connection
    )
ELSE()
    RECURSE(
        trunk_native_connection
        trunk_rpc_proxy_connection
    )
    IF (NOT SANITIZER_TYPE AND NOT OPENSOURCE)
        RECURSE(
            trunk_federated_connection
            23_2
            24_1
        )
    ENDIF()
ENDIF()
