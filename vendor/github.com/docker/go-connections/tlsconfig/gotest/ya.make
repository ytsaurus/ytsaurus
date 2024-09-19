GO_TEST_FOR(vendor/github.com/docker/go-connections/tlsconfig)

LICENSE(Apache-2.0)

DATA(
    arcadia/vendor/github.com/docker/go-connections/tlsconfig/fixtures
)

TEST_CWD(vendor/github.com/docker/go-connections/tlsconfig)

GO_SKIP_TESTS(
    TestConfigServerExclusiveRootPools
    TestConfigClientExclusiveRootPools
)

END()
