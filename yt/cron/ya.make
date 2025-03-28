RECURSE(
    clear_tmp
    prune_offline_servers
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_make_internal.inc)
ENDIF()
