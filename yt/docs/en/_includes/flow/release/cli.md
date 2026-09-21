# Flow CLI

Use the standard `yt` CLI tool with the `flow` mode to work with Flow [Pipeline](../../../flow/concepts/glossary.md#pipeline). This section lists the main commands. You can view all available options for each command by using the `--help` command.

#|
|| **Command** | **Description** ||
|| `start-pipeline` | Start the Pipeline ||
|| `stop-pipeline` | Stop the Pipeline using the `draining` mode ||
|| `pause-pipeline` | Stop the Pipeline immediately ||
|| `get-pipeline-state` | Get the current state of the Pipeline ||
|| `get-flow-view` | View the Flow View — a description of the entire pipeline from the controller's perspective ||
|| `get-pipeline-spec`, `set-pipeline-spec` | View or modify the current [Spec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec). Requires the Pipeline to be stopped ||
|| `get-pipeline-dynamic-spec`, `set-pipeline-dynamic-spec` | View or modify the current [DynamicSpec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec) ||
|| `read-states` | Read the pipeline states. Returns the `key_states`, `partition_states`, `external_key_states`, and `joined_external_key_states` sections. You can filter by `computation_id`, `partition_id`, `key`, `name`, or `target`; the `limit` applies to each section independently. See `TReadStatesArg` and `TReadStatesResponse` in the [reference](../../../flow/generated_docs/all_yson_structs.md) ||
|| `delete-states` | Delete the pipeline states. By default, it runs in dry-run mode: it returns the counts of matching rows without deleting them. Requires the Pipeline to be in the Stopped or Completed state, or `force=true` when Paused. Only key, partition, and manager states are deleted; joiner states are not affected. See `TDeleteStatesArg` and `TDeleteStatesResponse` in the [reference](../../../flow/generated_docs/all_yson_structs.md) ||
|#

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

- [Basic rollout rules](../../../flow/release/basic-rules.md)
- [Spec and DynamicSpec](../../../flow/concepts/spec.md)