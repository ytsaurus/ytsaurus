RECURSE(
    spyt-package/src/main
    python-examples/yamake_sample
    python-examples/direct_submit_example
    tools/release
)

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        e2e-test/ci
    )
ENDIF()
