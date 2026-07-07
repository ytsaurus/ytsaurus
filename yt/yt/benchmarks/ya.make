RECURSE(
    action_queue
    future
    context_switch
    exceptions
    fair_share_action_queue
    format_string
    formats
    insert
    io
    io_engine
    joining_reader
    logging
    mpsc_sharded_queue
    proto
    queues
    rng
    rpc
    sendfile
    skiff
    versioned_block_format
    yson_struct
)

# The bus benchmark can drive the UCX/RDMA transport, which is Linux-only and
# not built in OpenSource.
IF (NOT OPENSOURCE AND OS_LINUX)
    RECURSE(
        bus
    )
ENDIF()
