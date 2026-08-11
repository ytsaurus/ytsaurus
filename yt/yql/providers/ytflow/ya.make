RECURSE(
    expr_nodes
    integration
)

IF (NOT OPENSOURCE)
    RECURSE(
        codec
        common
        gateway
        job
        lambda_builder
        provider
    )
ENDIF()
