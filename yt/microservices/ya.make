RECURSE(
    bulk_acl_checker
    bulk_acl_checker_roren
    id_to_path_mapping
    resource_usage
    resource_usage_roren
)

IF (NOT OPENSOURCE)
    RECURSE(
        access_log_viewer
        error_manager
        excel
        firehose
        gang_tools
        grant_permissions
        hermes
        id_to_path_mapping
        klacalka
        lib
        ml
        perfetto
        resource-pumper
        solomon-resolver
        thor
    )
ENDIF()
