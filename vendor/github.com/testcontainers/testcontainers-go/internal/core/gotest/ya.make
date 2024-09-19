GO_TEST_FOR(vendor/github.com/testcontainers/testcontainers-go/internal/core)

LICENSE(MIT)

GO_SKIP_TESTS(TestFileExists)

DATA(
    arcadia/vendor/github.com/testcontainers/testcontainers-go/internal/core/testdata
)

TEST_CWD(vendor/github.com/testcontainers/testcontainers-go/internal/core)

END()
