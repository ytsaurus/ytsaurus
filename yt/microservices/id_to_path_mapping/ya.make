RECURSE(
    id_to_path_updater
)

IF (NOT OPENSOURCE)
    RECURSE(
        client
    )
ENDIF()
