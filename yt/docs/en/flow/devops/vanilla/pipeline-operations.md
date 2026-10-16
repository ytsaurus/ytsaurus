# Pipeline operations in {{product-name}} Flow

After the [initial deployment](initial-deploy.md), you manage the pipeline through the [CLI](../../tools/cli.md). The main operations are start, stop, and pause:

* `start-pipeline` — start the pipeline;
* `stop-pipeline` — stop the pipeline through `draining` mode (a full flush of the intermediate buffers);
* `pause-pipeline` — stop the pipeline immediately.

For more about pipeline states, see the [glossary](../../concepts/glossary.md#start-stop-pause-pipeline).

These commands control the pipeline state, not the Vanilla operation itself: stopping the operation and recreating it when a new release is deployed are described in [Updates and releases](releases.md).

## Remove a pipeline completely {#remove}

Removing a pipeline node recursively deletes the Flow internal tables under that node. User-managed external state tables outside the node remain and require a separate removal decision. Take `<operation-id>` from the pipeline's Vanilla operation page. Confirm that it and the pipeline path identify the deployment you intend to remove, then abort the operation and recursively remove the node:

```bash
yt --proxy <cluster> abort-op <operation-id>
yt --proxy <cluster> remove -r //path/to/pipeline
```

Immediately after aborting, `yt --proxy <cluster> remove -r` may report `Cannot take "exclusive" lock ... leader_controller_lock`. The former leader's master transaction still holds the lock. Wait for the lock count to become zero, then retry removal:

```bash
yt --proxy <cluster> get //path/to/pipeline/leader_controller_lock/@lock_count
```

The operation is complete when `yt --proxy <cluster> remove -r` succeeds and the pipeline node no longer exists.

## See also

- [{{product-name}} Flow CLI](../../tools/cli.md)
- [Initial deployment](initial-deploy.md)
- [Updates and releases](releases.md)
- [Security and access](security.md)
- [Spec and DynamicSpec](../../concepts/spec.md)
