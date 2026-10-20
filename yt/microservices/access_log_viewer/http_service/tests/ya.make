GO_TEST_FOR(yt/microservices/access_log_viewer/http_service)
END()

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        integration
    )
ENDIF()
