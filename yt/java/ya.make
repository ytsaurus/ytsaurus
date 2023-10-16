RECURSE(
    annotations
    skiff 
    type-info
    yson
    yson-tree
    ytsaurus-client
    ytsaurus-client-core
    ytsaurus-client-examples
    ytsaurus-testlib
)

IF (NOT OPENSOURCE)
    RECURSE(
        beam-ytsaurus
        beam-ytsaurus/play
        benchmarks
        canonize-schema
        jdbc
        jdbc/ub
        yson-json-converter
        ytclient
        ytclient/tutorial
    )
ENDIF()
