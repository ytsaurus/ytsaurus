       
RECURSE(
    cms
    dashboard_generator
    dashboards
)

IF (NOT OPENSOURCE)
    RECURSE(
        common
        core
        acl_dumper
        ytcfgen
        ytdyncfgen
        drive_monitor
        luigi
        infra_noc
        infra_cli
        scheduler_codicils
        shiva2
        hulk
        snapshot_processing
        yt-tag-table
        trash_recovery
        hwinfo
        devil-hulk
        stateless-service-controller
        gh_ci_vm_image_builder
        sevenpct
    )
ENDIF()
