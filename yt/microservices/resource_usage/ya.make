RECURSE(
    json_api_go
)

IF (NOT OPENSOURCE)
    RECURSE(
        common
        json_api
        local_run
        snapshot_importer
    )
ENDIF()
