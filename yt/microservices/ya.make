RECURSE(
    bulk_acl_checker
    bulk_acl_checker_roren
    resource_usage
    resource_usage_roren
    access_log_viewer
)

IF (NOT OPENSOURCE)
    RECURSE(
        error_manager
        excel
        firehose
        grant_permissions
        hermes
        id_to_path_mapping
        klacalka
        lib
        perfetto
        resource-pumper
        solomon-resolver
        thor
    )
ENDIF()
