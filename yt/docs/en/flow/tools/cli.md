# Flow CLI

Use the standard `yt` CLI tool with the `flow` mode to work with Flow [Pipeline](../concepts/glossary.md#pipeline). This section lists the main commands. You can view all available options for each command by using the `--help` command.

#|
|| **Command** | **Description** ||
|| `start-pipeline` | Start the Pipeline ||
|| `stop-pipeline` | Stop the Pipeline using the `draining` mode ||
|| `pause-pipeline` | Stop the Pipeline immediately ||
|| `get-pipeline-state` | Get the current state of the Pipeline ||
|| `get-flow-view` | View the Flow View — a description of the entire pipeline from the controller's perspective ||
|| `get-pipeline-spec`, `set-pipeline-spec` | View or modify the current [Spec](../concepts/glossary.md#spec-and-dynamic-spec). Requires the Pipeline to be stopped ||
|| `get-pipeline-dynamic-spec` | View the effective or base DynamicSpec ||
|| `set-pipeline-dynamic-spec`, `remove-pipeline-dynamic-spec` | Modify the base DynamicSpec until the next release ||
|| `get-pipeline-dynamic-spec-patches` | List or inspect active patches and the current state version ||
|| `patch-pipeline-dynamic-spec` | Create or update a durable DynamicSpec patch ||
|| `cancel-pipeline-dynamic-spec-patch` | Cancel one patch ||
|| `reset-pipeline-dynamic-spec` | Remove all patches and return to the release base ||
|| `read-states` | Read the pipeline states. Returns the `key_states`, `partition_states`, `external_key_states`, and `joined_external_key_states` sections. You can filter by `computation_id`, `partition_id`, `key`, `name`, or `target`; the `limit` applies to each section independently. See `TReadStatesArg` and `TReadStatesResponse` in the [reference](../generated_docs/all_yson_structs.md) ||
|| `delete-states` | Delete the pipeline states. By default, it runs in dry-run mode: it returns the counts of matching rows without deleting them. Requires the Pipeline to be in the Stopped or Completed state, or `force=true` when Paused. Only key, partition, and manager states are deleted; joiner states are not affected. See `TDeleteStatesArg` and `TDeleteStatesResponse` in the [reference](../generated_docs/all_yson_structs.md) ||
|#

## Durable DynamicSpec changes {#dynamic-spec-overrides}

The controller stores the DynamicSpec from each release as the base and keeps user patches separately. The effective DynamicSpec applies patches to the current base in order and accounts for the runtime target state. A release updates the base without dropping patches. The old `set-pipeline-dynamic-spec` and `remove-pipeline-dynamic-spec` commands modify only the base, so those changes can be replaced by the next release.

Read any layer and its shared version with `get-pipeline-dynamic-spec`. The default view is `effective`. Use `--view base` to read the base and `get-pipeline-dynamic-spec-patches` to list patches. `--blame` includes a map from paths to patch IDs, comments, and timestamps. A field uses its nearest ancestor entry; a later parent patch hides attribution from earlier child patches. Reading patches requires pipeline `read` permission; changing them requires `write`.

```bash
yt flow get-pipeline-dynamic-spec //home/project/pipeline --view base
yt flow get-pipeline-dynamic-spec //home/project/pipeline --view effective --blame
yt flow get-pipeline-dynamic-spec-patches //home/project/pipeline
yt flow get-pipeline-dynamic-spec-patches //home/project/pipeline --patch PATCH_ID
```

The patch listing returns `version` and active `patches` in application order. A patch has a generated `patch_id`, a comment, a timestamp, and one or more path/value changes. Changes are always listed in `changes=[{path,value},...]`, including patches with a single path. Each change replaces the value at its path. Patches at overlapping paths remain independent, including patches hidden by later ones. A new request creates a new patch even when it changes a path used by an earlier patch. Pass its `patch_id` to replace the complete set of changes in an existing patch. Updating values moves the patch to the end of the application order and updates its timestamp; updating only a comment preserves both timestamp and order. Equal values are still pinned: a later release may change the base.

Use the version you just read as `CURRENT_VERSION`. A nonempty comment is required when creating a patch. Every patch mutation checks `--expected-version` and fails atomically if the state has changed.

```bash
yt flow patch-pipeline-dynamic-spec //home/project/pipeline \
  --set /computations/reader/parameters/desired_partition_count 8 \
  --comment 'Increase reader parallelism' --expected-version CURRENT_VERSION
```

The UI's full-spec editor computes the changed paths and sends them together as one atomic `patch-pipeline-dynamic-spec` request. Management clients can send `set_changes` with a list of `{path, value}` entries, plus `comment` and `expected_version`. Unchanged fields of an edited map are not included and receive no patch blame. Removing fields is not supported.
The CLI accepts repeated `--set PATH VALUE` arguments to create one patch with several changes.

The response contains a new state `version` and the `patch_id`. Cancellation removes only the selected patch, then recalculates the effective spec from the base and all remaining patches. A previously hidden patch takes effect again unless another patch still masks it. Reset removes every patch but preserves the base and runtime target state.

```bash
yt flow cancel-pipeline-dynamic-spec-patch //home/project/pipeline \
  --patch PATCH_ID --expected-version CURRENT_VERSION
yt flow reset-pipeline-dynamic-spec //home/project/pipeline \
  --expected-version CURRENT_VERSION
```

Patches cannot change `target_state`. `start-pipeline`, `stop-pipeline`, and `pause-pipeline` manage that state independently. The complete stored snapshot must fit the YSON nesting and configured size limits; a rejected request leaves the previous state intact.

### Recovering from an incompatible release {#dynamic-spec-recovery}

If the saved DynamicSpec with patches fails validation in a new release and prevents the controller from starting, you cannot repair it through that controller's API. To recover:

1. Run the pipeline on a compatible release that supports patches and can read the saved DynamicSpec. This is usually the previous working release.
2. Read the base and the patch list. Fix incompatible values in the base or in the patches themselves, preparing a configuration accepted by both the current and the new release. Changing only the base does not fix a value overridden by a patch. Cancel a patch only if the pipeline can operate without it.
3. Deploy the new release again. It updates the base and applies the saved patches; verify that the resulting DynamicSpec passes validation and the pipeline starts.
4. On the new release, update DynamicSpec to the required configuration, including parameters that the compatible release did not support. Before changing patches, read `version` again and pass it as `--expected-version`.

A rollback alone does not remove patches. Fix the incompatibility before deploying again, then check the effective DynamicSpec after deployment.

## Direct runner commands {#direct-controller-commands}

Releasing a pipeline, the runner (`flow_server` or the pipeline binary built on it) sends the controller a handful of commands: it queries the pipeline state, stops or pauses the pipeline, updates the flow core target and the specs, starts the pipeline and waits for the state again. By default all of them travel through the RPC proxy.

When the proxy cannot connect to the controller, such a release fails with `Cannot connect to pipeline controller leader`, and the controller logs that the leadership confirmation through the proxy is skipped and that only the runner reaches it in the direct mode. In that case enable the direct mode: the runner sends the same commands to the controller itself.

Enable the direct mode with the `direct_controller_commands` block of the runner config:

```yson
direct_controller_commands = {
    enabled = %true;
};
```

Block settings:

- `enabled` — turns the direct mode on; `%false` by default.
- `rpc_timeout` — the timeout of a single command; 30 seconds by default, as for the requests through the RPC proxy.

The connection to the cluster stays as it is: the runner reads the controller address through it, and the controller validates the credentials against the same cluster. Encrypting the connection to the controller is not supported yet.

Limitations:

- The permissions are the same as on the proxy path: `read` on the pipeline to query its state and `write` to change it. The runner uses the same credentials it uses for the cluster (`YT_TOKEN`, `~/.yt/token`).
- The runner needs network access to the controller's RPC port.
- The direct mode does not replace the proxy signature check. While the controller runs with `require_proxy_signature = %false`, any host that reaches its RPC port runs commands unauthenticated, past the checks of the direct mode. Open the port only together with `require_proxy_signature = %true`.
- Unless the controller's `bus_server` is configured with TLS, the token reaches the controller over a plain connection. The pipeline process, which runs under the credentials of the pipeline owner, sees the tokens of everyone who uses the mode. Allow access to the controller's port only from the hosts that need it, and use the proxy path wherever it works.
- Only the runner has the direct mode. The SDK launchers — the Java runner, for example — start `flow_server` with the same config, so the mode works through them as well and needs no support of its own. The companion SDKs will not get it: the pipeline commands are sent by the runner. The `yt` CLI and the UI have no direct mode yet.

## See also

- [Basic rollout rules](../devops/vanilla/releases.md#release-and-configure-basic-rules)
- [Spec and DynamicSpec](../concepts/spec.md)
