RECURSE(
    rpc_proxy_sample
)

IF (OPENSOURCE_PROJECT OR NOT OPENSOURCE)
    RECURSE(
        comment_service
    )
ENDIF()
