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

## See also

- [Basic rollout rules](../../../flow/release/basic-rules.md)
- [Spec and DynamicSpec](../../../flow/concepts/spec.md)