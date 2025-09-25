RECURSE(
    client_go
    http_service_go
    lib_go
)

IF (NOT OPENSOURCE)
    RECURSE(
        client
        http_service
        lib
        preprocessing
    )

    RECURSE_FOR_TESTS(
        tests
    )
ENDIF()
