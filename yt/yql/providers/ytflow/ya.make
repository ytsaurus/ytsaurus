RECURSE(
    expr_nodes
    integration
)

IF (NOT OPENSOURCE)
    RECURSE(
        codec
        common
        comp_nodes
        gateway
        job
        lambda_builder
        provider
    )
ENDIF()
