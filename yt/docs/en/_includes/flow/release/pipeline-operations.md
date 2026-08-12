# Pipeline operations in {{product-name}} Flow

After the [initial deployment](../../../flow/release/launch-vanilla.md), you manage the pipeline{% if audience == "internal" %} through the {{product-name}} UI or{% endif %} through the [CLI](../../../flow/release/cli.md). The main operations are start, stop, and pause:

* `start-pipeline` — start the pipeline;
* `stop-pipeline` — stop the pipeline through `draining` mode (a full flush of the intermediate buffers);
* `pause-pipeline` — stop the pipeline immediately.

For more about pipeline states, see the [glossary](../../../flow/concepts/glossary.md#start-stop-pause-pipeline).

These commands control the pipeline state, not the Vanilla operation itself: stopping the operation and recreating it when a new release is deployed are described in [Updates and releases](../../../flow/release/releases.md).

## See also

- [{{product-name}} Flow CLI](../../../flow/release/cli.md)
- [Initial deployment](../../../flow/release/launch-vanilla.md)
- [Updates and releases](../../../flow/release/releases.md)
- [Security and access](../../../flow/release/security.md)
- [Spec and DynamicSpec](../../../flow/concepts/spec.md)
