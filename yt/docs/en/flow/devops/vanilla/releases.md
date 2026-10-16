# Releases in {{product-name}} Flow

## Updating the pipeline {#update}

To roll out a new version, change the code or the spec and run the same command again. A vanilla launch uses the make-before-break strategy: the new operation is prepared (the binary is uploaded to the {{product-name}} cache) while the old one keeps running, and then the switchover happens — the old operation finishes and the already prepared new one starts. Downtime is therefore minimal, and you don’t need to stop the pipeline separately before a release.

The way the **old** operation finishes is controlled by the `YT_FLOW_GRACEFUL_UPDATE` environment variable: `1` (the default) drains the old pipeline (`stop`), `0` pauses it (`pause`); after that, the old operation is aborted.

## Reanimating the operation {#reanimate}

The pipeline state (for example, `working`) is stored in [Cypress](../../../user-guide/storage/cypress.md) separately from the vanilla operation and doesn’t change when the operation finishes. So if the operation was aborted bypassing the launcher (manually or by an external system), the pipeline stays in a working state but makes no progress — there is nobody to run it. You can bring such a pipeline back up with **exactly the same version** using the `reanimate_vanilla_operation` tool. At launch, the launcher saves a manifest in the pipeline node containing the operation spec and references to its files. The tool reads that manifest and resubmits the operation without a rebuild:

```bash
ya run yt/yt/flow/tools/reanimate_vanilla_operation -- \
    --cluster <cluster> --path //path/to/pipeline
```

The tool **refuses to run if the operation is still alive** (so that a second one isn’t started for the same pipeline), and it takes `YT_TOKEN` and the values of the variables listed in `secret_env` from the launch environment again — they aren’t stored in Cypress.

## Hotfix {#hotfix}

To roll out a hotfix quickly, use `YT_FLOW_GRACEFUL_UPDATE=0`: the old operation is then paused (`paused`) before the replacement instead of being drained to `stopped`. This is acceptable if the following conditions are met:

* The difference between the old and the new version is minimal and easy to verify. Ideally &mdash; a single commit.
* There are no changes to the pipeline topology or to the stream schemas.
* The new version is ready to process the intermediate messages left over from the old version.
* The new version preserves deterministic behavior in [Swift](../../concepts/glossary.md#swift) computations — otherwise, without draining, intermediate messages may be lost or duplicated.

If the hotfix also changes the table configuration in {{product-name}}, follow the rules in [Table configuration during releases](#yt-sync) first.

A release also relies on `FlowCoreTarget` to keep processes built from a different commit out of the pipeline — see [Protection against zombie processes](#flow-core-target).

## Recovering from a bad spec {#recovery-from-bad-spec}

If the pipeline jobs fail every epoch (for example, because of an error in the static spec), the pipeline cannot drain and stays in the `draining` state, while by default an update first stops the pipeline and only then uploads the new spec. A fix cannot be rolled out the usual way in this situation — the launch fails on timeout with an error like `Timed out after ... waiting for pipeline state "stopped"`.

The same error appears when the pipeline simply takes longer than the timeout to drain (a large backlog, a source that has not gone empty yet). Before disabling the drain, confirm from the job logs that the pipeline really cannot drain: pausing leaves intermediate messages in the queues, which is exactly what draining exists to avoid.

Roll out the fix in two steps:

1. Run the update with `YT_FLOW_GRACEFUL_UPDATE=0`: the pipeline is paused instead of stopped, and the new spec is applied without draining. This update is subject to the [hotfix](#hotfix) constraints, including the ban on changing the topology and the stream schemas, so make only the changes that bring the jobs back to a working state.
2. Once the jobs work again and the pipeline is able to drain, roll out the topology and stream schema changes with a regular update that drains.


## Table configuration during releases {#yt-sync}

Compare the existing tables, schemas, and attributes with the objects required by the new spec. Apply table changes before pushing a release whose spec needs them. The [Wait Click Join example](../../cpp/examples/wait_click_join.md#yt-sync) shows the relevant table setup. A missing table can prevent the new pipeline version from starting.

{% if audience == "internal" %}

For internal deployments, describe the required tables in [YtSync]({{yt-sync-docs}}/) and inspect its `dump-diff` before applying changes. Keep the table migration as a deliberate release step; the runner does not migrate user tables for you.

{% endif %}

### Release and table change rules {#release-and-configure-basic-rules}

Inspect the table diff before changing the static spec or executable: the new version may need tables that do not exist yet. A release can also change Flow's internal table schemas. Stop the pipeline completely before every migration, especially changes to internal tables. Two narrow exceptions can reduce downtime: create a new user table before the new version uses it, or delete an old user table after the update once nothing uses it. Apply the same timing to new or unused columns only when you have verified compatibility. Avoid releases and table changes during cluster maintenance; keep production and test schemas aligned where practical.

## Version mismatch and zombie processes {#flow-core-target}

Build the runner, controller, and workers from the same commit. The runner normally sets `FlowCoreTarget` when it pushes a spec. A process with another `FlowCoreVersion` cannot continue as an active member, which prevents an old worker from silently processing a new release.

### How the target works {#flow-core-target-how-it-works}

When versions disagree, the pipeline can pause and show `binary mismatch` in **Messages**; a spec update may fail with `FlowCoreTargetMismatch`. Inspect the controller and worker logs and the [flow view](../../tools/cli.md); compare the deployed binary revisions. A temporary mismatch during rollout may clear when the new operation replaces all old jobs.

### Manage the target {#flow-core-target-how-to-set}

Leave automatic target updates enabled for a normal release. For a custom runner, set the target to its binary's `FlowCoreVersion` when pushing the spec.

#### Custom runner {#flow-core-target-custom-runner}

A runner based on `TSimpleRunnerProgram` sets the target automatically. A runner built independently must write its own binary's `FlowCoreVersion` when it publishes the spec, so the controller and workers agree on the expected version.

#### Disable automatic updates {#flow-core-target-disable-auto}

Use `set_flow_core_target = %false` in the runner config for a persistent override. The C++ and Java launchers also forward `--skip-set-flow-core-target` for one launch; the Python and Go launchers ignore that flag, so use the config setting with them. Restore automatic updates after the exceptional release.

#### Inspect or set the target {#flow-core-target-manual}

```bash
{{yt-cli}} flow execute <pipeline_path> get-flow-core-target --input-format json '{}'
{{yt-cli}} flow execute <pipeline_path> set-flow-core-target --input-format json '{"flow_core_target":"<target>"}'
```

The pipeline must be stopped to change the target. On a paused pipeline, a change requires `"allow_update_on_pause": true` in the request body.

#### Reset version checking {#flow-core-target-workarounds}

Resetting the target lets processes of any version participate, so use it only to recover a trusted deployment. Pause the pipeline, reset with `"allow_update_on_pause": true`, then restart it after checking the deployed binaries:

```bash
{{yt-cli}} flow pause-pipeline <pipeline_path>
{{yt-cli}} flow execute <pipeline_path> set-flow-core-target --input-format json '{"flow_core_target":"","allow_update_on_pause":true}'
{{yt-cli}} flow start-pipeline <pipeline_path>
```

The next runner launch will set a new target unless automatic updates are disabled in its config or, for C++ and Java only, `--skip-set-flow-core-target` is used.

#### CI and rollbacks {#flow-core-target-cicd}

Run the runner from the same build artifact as the controller and workers. During rollback, use the older runner with the older binaries so that it writes the matching target. Do not reuse a developer runner against a production pipeline.

### Investigate a persistent mismatch {#flow-core-target-troubleshooting}

If the pipeline stays paused after rollout, compare the revisions of all live processes and look for jobs from the previous operation. Check the operation and job logs before retrying the release. Do not clear version checking while old processes can still reach the pipeline.

#### Version identity {#flow-core-version-source}

A build with VCS metadata derives `FlowCoreVersion` from its commit. Without VCS metadata, it derives the version from the binary checksum; rebuilding with different flags can therefore produce a different version.

## Deployment and launch guides {#launch-flow}

<span id="deployment-pages"></span>

For deployment options and operating guides, see [Operating Flow](../index.md#deployment-pages).

## See also

- [Initial deployment](initial-deploy.md)
- [Basic pipeline operations](pipeline-operations.md)
- [Security and access](security.md)
- [Spec and DynamicSpec](../../concepts/spec.md)
- [Pipeline CLI](../../tools/cli.md)
