RECURSE(
    annotations
    skiff 
    type-info
    yson
    yson-tree
    yson-json-converter
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
        ytclient
        ytclient-core
        ytclient/tutorial
    )
ENDIF()
