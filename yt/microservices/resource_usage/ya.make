RECURSE(
    json_api_go
    tests
)

IF (NOT OPENSOURCE)
    RECURSE(
        common
        json_api
        local_run
        snapshot_importer
    )
ENDIF()
