RECURSE(
    agent
    plugin
)

IF(NOT EXPORT_CMAKE)
    RECURSE_FOR_TESTS(
        providers
        tools
    )
ENDIF()

IF (NOT OPENSOURCE)
    RECURSE(
        purecalc
    )
ENDIF()

# TODO(gritukan): These tests will be broken in open source after this commit,
# but we are going to implement test infrastructure for YQL in the next commit.
RECURSE_FOR_TESTS(tests)
