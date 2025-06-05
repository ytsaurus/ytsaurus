RECURSE(
    expr_nodes
    integration
)

IF (NOT OPENSOURCE)
    RECURSE(
        codec
        gateway
        job
        lambda_builder
        provider
    )
ENDIF()
