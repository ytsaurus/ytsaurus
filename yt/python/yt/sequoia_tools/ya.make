PY3_LIBRARY()

PEERDIR(
    yt/python/yt/record_codegen_helpers
    library/python/resource
    contrib/python/dacite
    contrib/python/pyaml
)

PY_SRCS(
    NAMESPACE yt.sequoia_tools

    __init__.py
)

RESOURCE_FILES(
    yt/yt/ytlib/sequoia_client/records/child_node.yaml
    yt/yt/ytlib/sequoia_client/records/chunk_replicas.yaml
    yt/yt/ytlib/sequoia_client/records/dependent_transactions.yaml
    yt/yt/ytlib/sequoia_client/records/location_replicas.yaml
    yt/yt/ytlib/sequoia_client/records/unapproved_chunk_replicas.yaml
    yt/yt/ytlib/sequoia_client/records/node_id_to_path.yaml
    yt/yt/ytlib/sequoia_client/records/path_to_node_id.yaml
    yt/yt/ytlib/sequoia_client/records/transactions.yaml
    yt/yt/ytlib/sequoia_client/records/transaction_descendants.yaml
    yt/yt/ytlib/sequoia_client/records/transaction_replicas.yaml
    yt/yt/ytlib/sequoia_client/records/node_forks.yaml
    yt/yt/ytlib/sequoia_client/records/node_snapshots.yaml
    yt/yt/ytlib/sequoia_client/records/path_forks.yaml
    yt/yt/ytlib/sequoia_client/records/child_forks.yaml
    yt/yt/ytlib/sequoia_client/records/response_keeper.yaml
    yt/yt/ytlib/sequoia_client/records/chunk_refresh_queue.yaml
)

END()
