## General principles {#principles}

Use these principles when writing integration tests for pipelines:

* Test end-to-end scenarios. That is:
  * Write input/source data to the local YT/LB.
  * In the pipeline spec, mark the sources as `finite=%true`.
  * Run the pipeline.
  * Wait for the pipeline to finish.
  * Check the output data.
* Prepare the environment with the same code you use in production:
  * Test the same pipeline binary that will run in production.
  * Generate pipeline specs with the same code that generates them for production.
{% if audience == "internal" %}  * Use the production binary [YtSync]({{yt-sync-docs}}/), which includes a dedicated stage for tests.{% endif %}
  * If input/output data require non-trivial serialization/parsing, implement this logic with code shared with production.
* Use the [shared test framework]({{source-root}}/yt/yt/flow/library/python/integration_test_base) (a `README.md` is available).
* Implement a failover test.
  * Many errors surface when workers fail and other workers take over their tasks. So, run a test with `problems=True` and more than one worker.
* Write stable tests.
  * Remember that in CI, any part of a test might run unexpectedly long.
    * The ideal runtime for a single test is 20 seconds for any build type.
    * For sanitizer builds, reduce the amount of input data.
    * Set all local timeouts with a multiple safety margin.
  * The test execution logic shouldn’t depend significantly on the current time. For example, a test shouldn’t fail if it starts before midnight and finishes after.

{% if audience == "internal" %}

## YtSync in tests {#yt-sync}

Run [YtSync]({{yt-sync-docs}}/) in user tests via the test framework’s `run_yt_sync_ensure` method, as shown in the [example]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/test/test_wait_click_join.py#L142). This method starts the YtSync executable with parameters that improve YtSync’s efficiency and with the required environment variables. Running YtSync via `yatest.common.execute` is also allowed, but avoid this option unless necessary.

If a pipeline exists solely for tests to validate some YT Flow functionality, use the _built-in_ YtSync, as shown in the [example]({{source-root}}/yt/yt/flow/tests/flow_execute/test_flow_execute.py#L33).

{% endif %}

## Debugging tests {#debug}

### Logs {#debug-logs}

After a test finishes, you can find its logs in `test-results/py3test/testing_out_stuff` in the test directory. Key logs include:

* `run.log` — Python test logs.
* `<test_class_name>/<test_name>/Controller_<number>...` — Controller logs (`.err` is the process’s stderr; `.log` contains regular logs written via `YT_LOG_...`).
* `<test_class_name>/<test_name>/Worker_<number>...` — Worker logs.
* `<test_class_name>/<test_name>/Runner...` — Runner logs.

If something isn’t working, check errors in all these logs. The approach to reviewing them is the same as in production — see [Logs](../../flow/release/logs.md).

You can also view logs before the test finishes. To do this, locate the temporary directory where the test runs. The simplest way is to run the test with the `--keep-temps` flag: `ya make --keep-temps -ttt <target>`. In this case, `ya make` won’t delete the temporary directory after the test finishes and will print a link to it in the output.

You can also use these bash aliases:

```bash
alias curtestdir="ps -f -u $USER | python3 -c \"import sys, re; drs = set(e for e in re.findall(r'[\s=](/\S*testing' + r'_out_stuff)\b', sys.stdin.read())); print('' if len(drs) == 1 else 'Select first from ' + repr(drs), file=sys.stderr); print(list(drs)[0])\""
alias cdcurtestdir='cd $(curtestdir)'

# Get the link to the local {{product-name}} UI from the logs.
alias curlocalyt='cat $(curtestdir)/stderr 2>/dev/null | grep YT'
```

### Local {{product-name}} UI {#debug-ui-yt}

You need network access from the `_YTFRONT_PROD_NETS_` network to the machine where the test runs, on TCP ports 81–65535. This lets you use the production and beta UIs. If you want to use the test UI, you also need network access from the `_DATAUI_INFRASTRUCTURE_NETS_` network to the same ports.

In this case, you can open the regular {{product-name}} UI to interact with the pipeline running on the local {{product-name}}. You can extract the local cluster UI address from the logs; see the bash aliases in the previous section.

To debug with the UI, you need long timeouts in your tests (or disable them entirely).

### Test framework {#debug-test-framework}

You can read about how to set up the environment for better test framework performance and how to influence test parameters in the framework’s [README.md]({{source-root}}/yt/yt/flow/library/python/integration_test_base).