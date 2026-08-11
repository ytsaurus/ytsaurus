# Run a pipeline in a Vanilla operation

This is the simplest way to run Flow: you don’t need a separate long-running deployment of controllers and workers — they start inside a single {{product-name}} [Vanilla operation](../../../user-guide/data-processing/operations/vanilla.md). To enable this type of run, just add the `vanilla` block to `pipeline.yson`.

## What you’ll need {#prerequisites}

You need a configuration file:

* `pipeline.yson` — [runner config](../../../flow/concepts/spec.md#config) with the pipeline spec. For the pipeline to run in a Vanilla operation, it must include a `vanilla` block with `enable = %true` (see [How to enable](#enable)). You don’t need a separate `config.yson` — the node config inside the jobs is built automatically.

And binaries — their roles depend on the language:

{% list tabs %}

- C++

  * `pipeline` — your pipeline binary. It also acts as `flow_server` (via `TSimpleRunnerProgram`) and works as a controller, worker, and runner.

- Python

  * `pipeline` — a lightweight Python binary: launcher plus companion.
  * `flow_server` — the Flow server binary (`yt/yt/flow/bin/flow_server`) that works as a controller and worker; its path is passed to the runner via `--flow-bin`. The companion is delivered to the job automatically.

- Java

  * `pipeline` — a jar launcher plus companion.
  * `flow_server` — the Flow server binary (`yt/yt/flow/bin/flow_server`) that works as a controller and worker; its path is passed to the runner via `--flow-bin`. The companion is delivered to the job automatically.

{% endlist %}

## How to enable {#enable}

Add the `vanilla` block to your pipeline config:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "worker" = {"count" = 5};
};
```

The required parameters are `pool` and `worker.count`. Other fields have sensible default values: the number and resources of the controller and worker jobs, while {{product-name}} assigns the ports itself via `YT_PORT_*`. If needed, you can override the resources explicitly:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "controller" = {"count" = 1; "cpu_limit" = 6; "memory_limit" = "18g"};
    "worker" = {"count" = 5; "cpu_limit" = 6; "memory_limit" = "18g"};
};
```

For the full list of fields, see [TVanillaConfig](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaConfig) and [TVanillaTaskConfig](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaTaskConfig).

When you run it, `flow_server` validates the spec itself, creates a Vanilla operation with two tasks (controller and worker), sets the pipeline spec, and starts it.

## Run the pipeline {#run}

{% list tabs %}

- C++

  ```bash
  ./pipeline --config pipeline.yson
  ```

- Python

  ```bash
  ./pipeline --config pipeline.yson --flow-bin flow_server
  ```

- Java

  ```bash
  ./pipeline RunnerMain --config pipeline.yson --flow-bin flow_server
  ```

{% endlist %}

### Validate the spec only {#validate-only}

To locally check the spec’s correctness without sending it to the controller, pass the `--validate-only` flag to the runner. The runner will perform all checks (parsing and validating the static and dynamic spec) and then exit. If the spec is invalid, the runner exits with an error; otherwise, it exits successfully.

```bash
./pipeline --config pipeline.yson --validate-only
```

## Update the pipeline {#update}

To roll out a new version, change the code or spec and run the same command again. Vanilla runs use a make-before-break strategy: the new operation is prepared (the binary is loaded into the {{product-name}} cache) while the old one keeps running. Then a switchover happens — the old operation ends, and the prepared new one starts. This keeps downtime minimal, so you don’t need to stop the pipeline separately before release.

The way the **old** operation ends is controlled by the `YT_FLOW_GRACEFUL_UPDATE` environment variable: `1` (default) means the old pipeline is drained (`stop`), `0` means it’s paused (`pause`); after that, the old operation is canceled.

## Reanimate the operation {#reanimate}

The pipeline state (for example, `working`) is stored in [Cypress](../../../user-guide/storage/cypress.md) separately from the Vanilla operation and doesn’t change when the operation ends. So if you canceled the operation outside the launcher (manually or by an external system), the pipeline stays in a working state but makes no progress — there’s no one to run it. You can restart such a pipeline with **exactly the same version** using the `reanimate_vanilla_operation` tool. When the launcher runs, it saves a manifest (the operation spec and links to its files) in the pipeline’s Cypress node; the tool reads this manifest and resubmits the operation without requiring a rebuild:

```bash
ya run yt/yt/flow/tools/reanimate_vanilla_operation -- \
    --cluster <cluster> --path //path/to/pipeline
```

The tool **refuses to run if the operation is still alive** (to avoid launching a second one for the same pipeline), and it rebuilds secrets (`YT_TOKEN`, declared in `secret_env`) from the launch environment — they aren’t stored in Cypress.

## Hotfix {#hotfix}

To quickly roll out a hotfix, use `YT_FLOW_GRACEFUL_UPDATE=0`: then, before the swap, the old operation is paused (`Paused`) instead of being drained to `Stopped`. This is acceptable only if you meet these conditions:

* The change between the old and new version is minimal and easy to verify. Ideally, it’s just a single commit.
* There are no changes to the pipeline topology or stream schemas.
* The new version is ready to process intermediate messages left over from the old version.
* The new version preserves deterministic behavior in [Swift](../../../flow/concepts/glossary.md#swift) computations — otherwise, skipping the drain could cause losses or duplicates of intermediate messages.

## Secrets {#secrets}

To pass secrets to jobs (for example, {% if audience == "internal" %}`TVM_SECRET` for accessing Logbroker or{% endif %} your own token), list the names of the corresponding environment variables in `secret_env`:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "worker" = {"count" = 5};
    "secret_env" = ["TVM_SECRET"];
};
```

The values are taken from the launch environment (from the user who runs the command), passed to the operation’s `secure_vault` (they don’t go into Cypress or the spec), and are available inside the job as regular environment variables. You don’t need to specify `YT_TOKEN` — it’s delivered automatically.

## Authentication {#auth}

A Vanilla operation runs under the user who executed the command — that same user must have access to {{product-name}} (`YT_TOKEN` or TVM). You don’t need to configure authentication for controllers and workers separately, as you would for a long-running deployment.

## See also

- [Basic rollout rules](../../../flow/release/basic-rules.md){% if audience == "internal" %}
- [Launch via Infractl](../../../flow/release/launch-infractl.md){% endif %}
- [Spec and DynamicSpec](../../../flow/concepts/spec.md){% if audience == "internal" %}
- [Integration with Monitoring](../../../flow/release/monitoring.md#vanilla-monitoring){% endif %}