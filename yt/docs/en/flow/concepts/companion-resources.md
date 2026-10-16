# Companion resources: brief design

A companion resource is a heavyweight object managed by Flow that lives in a companion process and is available to user process functions. Examples include an unpacked dictionary, model, or connection pool. The resource belongs to the whole companion process rather than a single job, so it must not hold mutable job state and must support concurrent use.

## Components {#components}

- The resource controller determines the desired revision and publishes it to Flow.
- `TCompanionResource` on a worker acts as a proxy: it prepares the revision, manages its lifecycle, and sends commands to the companion.
- `TResourceStore` in the C++ companion stores resources by `resource_id`. The store belongs to the process and survives job creation and removal.
- A process function receives only resources listed in `required_resource_ids`, including aliases and transitive dependencies.

## Delivery and references {#delivery-and-references}

The worker calls `ResourceExecute(resource_id, command, argument)` over RPC. The command is an enum with two values: `init` and `unload`. The result contains a separate resource-command status and a structured `TError`.

The main path uses idempotent `init`. Its argument contains the static and dynamic specs, the prepared revision, incarnation identifier and generation, configuration generation, and exact dependency references. Dependencies are initialized before the resource that depends on them. `TJobInfo` receives the same exact references: `resource_id`, `incarnation_id`, `configuration_generation`, and an optional alias. Before user code starts, the companion checks each reference and exposes only matching instances to the process function.

## Reconfiguration and recovery {#reconfiguration-and-recovery}

- A new target revision is first prepared asynchronously on the worker through `PrepareResourceRevision`; a failed result is not published to the companion.
- Successful preparation increments the configuration generation and starts a convergent `init`.
- The companion publishes the new generation only after the resource applies the target revision. By contract, `Reconfigure` may only pass the target to an asynchronous switch. While `GetRevisionState` reports an applied revision that lags behind, `init` returns a temporary `resource_not_initialized`; retries converge without recreating the instance.
- A monotonic incarnation generation prevents a delayed command from reviving an old instance or removing a new one.
- The worker periodically retries `init`, and each computation delivers its required resources through its own channel. This restores state after the companion process restarts.
- If `ProcessBatch` returns `RESOURCE_NOT_INITIALIZED`, the worker reinitializes the resource graph, refreshes the references and job information, and retries the request a bounded number of times.
- `unload` is best effort. Running jobs retain references to the old object, but new jobs no longer receive it.

A complete server-side implementation currently exists in the C++ companion. Java, Go, and Python currently return `Unsupported` from `ResourceExecute`.
