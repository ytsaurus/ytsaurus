RECURSE(
    generate_tutorial
)

IF (NOT OPENSOURCE)
    RECURSE(
        upload_tutorial_data_from_sandbox
        generate_tutorial_data
    )

ENDIF()
