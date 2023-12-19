RECURSE(
    agent
    plugin
)

IF (NOT OPENSOURCE)
    # We cannot run tests in open source since we do not have YQL plugin shared library in our repo.
    RECURSE_FOR_TESTS(tests)
ENDIF()
