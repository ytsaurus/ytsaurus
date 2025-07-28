RECURSE(
    clear_tmp
    prune_offline_cluster_nodes
    snapshot_processing
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_make_internal.inc)
ENDIF()
