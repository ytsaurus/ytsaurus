RECURSE(
    clear_tmp
    prune_offline_cluster_nodes
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_make_internal.inc)
ENDIF()
