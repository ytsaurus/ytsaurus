# Releases in {{product-name}} Flow

## Updating the pipeline {#update}

To roll out a new version, change the code or the spec and run the same command again. A vanilla launch uses the make-before-break strategy: the new operation is prepared (the binary is uploaded to the {{product-name}} cache) while the old one keeps running, and then the switchover happens — the old operation finishes and the already prepared new one starts. Downtime is therefore minimal, and you don’t need to stop the pipeline separately before a release.

The way the **old** operation finishes is controlled by the `YT_FLOW_GRACEFUL_UPDATE` environment variable: `1` (the default) drains the old pipeline (`stop`), `0` pauses it (`pause`); after that, the old operation is aborted.

## Reanimating the operation {#reanimate}

The pipeline state (for example, `working`) is stored in [Cypress](../../../user-guide/storage/cypress.md) separately from the vanilla operation and doesn’t change when the operation finishes. So if the operation was aborted bypassing the launcher (manually or by an external system), the pipeline stays in a working state but makes no progress — there is nobody to run it. You can bring such a pipeline back up with **exactly the same version** using the `reanimate_vanilla_operation` tool. At launch, the launcher records two things in the pipeline node: a pointer to the operation it started and, separately, the spec that operation was submitted with (its files, layout, and the names of the variables in `secret_env`). The tool reads both and resubmits the same operation without requiring a rebuild:

```bash
ya run yt/yt/flow/tools/reanimate_vanilla_operation -- \
    --cluster <cluster> --path //path/to/pipeline
```

The tool **refuses to run if the operation is still alive** (so that a second one isn’t started for the same pipeline), and it takes `YT_TOKEN` and the values of the variables listed in `secret_env` from the launch environment again — they aren’t stored in Cypress.

## Hotfix {#hotfix}

To roll out a hotfix quickly, use `YT_FLOW_GRACEFUL_UPDATE=0`: the old operation is then paused (`Paused`) before the replacement instead of being drained to `Stopped`. This is acceptable if the following conditions are met:

* The difference between the old and the new version is minimal and easy to verify. Ideally &mdash; a single commit.
* There are no changes to the pipeline topology or to the stream schemas.
* The new version is ready to process the intermediate messages left over from the old version.
* The new version preserves deterministic behavior in [Swift](../../../flow/concepts/glossary.md#swift) computations — otherwise, without draining, intermediate messages may be lost or duplicated.

If the hotfix also changes the table configuration in {{product-name}}, follow the rules in [YT synchronization rules](../../../flow/release/yt-sync-rules.md) first.

A release also relies on `FlowCoreTarget` to keep processes built from a different commit out of the pipeline — see [Protection against zombie processes](../../../flow/release/flow-core-target.md).

## Recovering from a bad spec {#recovery-from-bad-spec}

If the pipeline jobs fail every epoch (for example, because of an error in the static spec), the pipeline cannot drain and stays in the `Draining` state, while by default an update first stops the pipeline and only then uploads the new spec. A fix cannot be rolled out the usual way in this situation — the launch fails on timeout with an error like `Timed out after ... waiting for pipeline state "stopped"`.

The same error appears when the pipeline simply takes longer than the timeout to drain (a large backlog, a source that has not gone empty yet). Before disabling the drain, confirm from the job logs that the pipeline really cannot drain: pausing leaves intermediate messages in the queues, which is exactly what draining exists to avoid.

Roll out the fix in two steps:

1. Run the update with `YT_FLOW_GRACEFUL_UPDATE=0`: the pipeline is paused instead of stopped, and the new spec is applied without draining. This update is subject to the [hotfix](#hotfix) constraints, including the ban on changing the topology and the stream schemas, so make only the changes that bring the jobs back to a working state.
2. Once the jobs work again and the pipeline is able to drain, roll out the topology and stream schema changes with a regular update that drains.

## See also

- [Initial deployment](../../../flow/release/launch-vanilla.md)
- [Basic pipeline operations](../../../flow/release/pipeline-operations.md)
- [Security and access](../../../flow/release/security.md)
- [YT synchronization rules](../../../flow/release/yt-sync-rules.md)
- [Spec and DynamicSpec](../../../flow/concepts/spec.md)
- [Pipeline CLI](../../../flow/release/cli.md)
