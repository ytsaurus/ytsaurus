RECURSE(
    agent
    plugin
)

# TODO(gritukan): These tests will be broken in open source after this commit,
# but we are going to implement test infrastructure for YQL in the next commit.
RECURSE_FOR_TESTS(tests)
