GO_TEST()

LICENSE(Apache-2.0)

VERSION(v1.74.2)

GO_SKIP_TESTS(Test)

DATA(
    arcadia/vendor/google.golang.org/grpc/testdata
)

TEST_CWD(vendor/google.golang.org/grpc)

GO_XTEST_SRCS(
    ads_stream_ack_nack_test.go
    ads_stream_restart_test.go
    authority_test.go
    cds_watchers_test.go
    client_custom_dialopts_test.go
    dump_test.go
    eds_watchers_test.go
    fallback_test.go
    federation_watchers_test.go
    helpers_test.go
    lds_watchers_test.go
    loadreport_test.go
    rds_watchers_test.go
    resource_update_test.go
)

END()
