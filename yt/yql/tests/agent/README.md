# YQL agent integration tests

## Debug logs

Debug logging for the local YT cluster is disabled by default to keep test
artifacts reasonably small. To preserve the standard debug logs while
investigating a test failure, pass the `YT_RECIPE_DEBUG_LOGS` test parameter:

```bash
ya test -F '*test_qtworker.py::TestAgentWithInvalidMaxYqlVersionWithQtWorker*' --test-param YT_RECIPE_DEBUG_LOGS
```

Run the command from `yt/yql/tests/agent`. Enabling debug logs may produce a
large amount of test output and should only be used for local debugging.
