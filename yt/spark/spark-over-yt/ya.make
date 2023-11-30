RECURSE(
    data-source/src/main
    python-examples/yamake_sample
    tools/release
)

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        e2e-test/ci
    )
ENDIF()
