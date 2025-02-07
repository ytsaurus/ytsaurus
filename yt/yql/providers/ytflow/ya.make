RECURSE(
    expr_nodes
    integration
)

IF (NOT OPENSOURCE)
    RECURSE(
        codec
        gateway
        provider
    )
ENDIF()
