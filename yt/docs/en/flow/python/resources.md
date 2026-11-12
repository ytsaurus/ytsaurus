# Companion resources in Python

A companion resource holds an object shared by the jobs served by one Python process: for example, a client or reference data. Flow creates it from a registered factory and manages its lifecycle. Each companion process has its own instances, including when [Python CPU parallelism](getting-started.md#companion-process-count) uses several interpreters.

Use this API after configuring the [Python companion](getting-started.md). For the worker proxy, resource dependencies and computation bindings, follow [Companion resource wiring](../concepts/companion.md#companion-resources).

## Register a resource class {#registration}

Implement `FlowResource` from `yt.yt.flow.library.python.companion` and register it on your `Pipeline` before `pipeline.run()`:

- `pipeline.add_resource(resource_class)` uses the class as a zero-argument factory.
- `pipeline.add_resource(resource_class, factory)` uses your zero-argument factory with the name derived from the class.
- `pipeline.add_resource(name, factory)` registers an explicit implementation name; the factory is required.

Every factory call must return a fresh `FlowResource` instance. Different resource ids and replacement instances have independent lifecycles. Registering a factory does not load a resource.

For class-based registration, the implementation name is the class's own nonempty `flow_resource_class` attribute, or its fully qualified `module.qualname`. The attribute is not inherited from a base class. Put this exact name in `parameters.companion_resource_class`; it is distinct from the resource id and the computation alias. Duplicate registration names are rejected.

The maintained [resource companion]({{source-root}}/yt/yt/flow/tests/companion/resource/python/pipeline/__main__.py) demonstrates class-based naming, an explicit name, a dependency and a computation that reads the resource. Its [pipeline spec]({{source-root}}/yt/yt/flow/tests/companion/resource/python/pipeline.yson) supplies the corresponding bindings.

## Lifecycle and context {#lifecycle}

Override the hooks needed by your resource. Their `ResourceContext` argument provides the current configuration and initialized dependencies.

#|
|| **Hook** | **Contract** ||
|| `load(context)` | Initialize a fresh instance from the static parameters, dynamic parameters and dependencies. Keep construction lightweight and put resource initialization here. ||
|| `reconfigure(context)` | Apply changed dynamic parameters to the live instance. The default implementation leaves it unchanged. Read new values from the argument. ||
|| `unload()` | Release held objects and external resources. Cleanup must tolerate a partially completed `load`. ||
|#

`context.resource_id` is the id from `spec.resources`. `context.parameters` contains the static resource parameters, including `companion_resource_class`; `context.dynamic_parameters` contains the dynamic parameters. Both are dictionaries. `context.dependencies` maps each dependency alias to its initialized `FlowResource`; without an alias, the dependency id is the key. Dependencies stay held while the dependent instance is alive.

The context also provides the process-wide `http_client` and `https_client`; see [HTTP clients](http-client.md).

## Read a resource in user code {#access}

Call `ctx.get_resource(alias)` from the processing callback, where `ctx` is its runtime context. Declare the resource in the computation's `required_resource_ids`; lookup uses that requirement's `alias`, or the resource id if no alias is set. A missing binding raises `ValueError`.

Resolve the resource again for each batch. Do not retain an instance in a computation or global variable across batches: Flow can retire it and run its cleanup before a later batch uses that cached reference.

## Concurrency and failures {#concurrency-and-failures}

An instance can serve multiple jobs and threads concurrently. Synchronize access to mutable state inside your resource. Reconfiguration refuses new acquisitions while the hook runs, but can overlap batches and dependent resources that already hold the instance. A batch lease postpones cleanup; it does not serialize mutations or provide a configuration snapshot.

When `load` fails after the factory returned an instance, Flow attempts `unload` on that partial instance. A failed `reconfigure` removes the instance from service; a later successful initialization builds a fresh instance. Cleanup waits for existing holders to release it. An `Exception` from `unload` is logged and swallowed; other `BaseException` subclasses propagate.

The worker retries initialization according to the [proxy configuration](../concepts/companion.md#companion-resource-failures). Make initialization safe to repeat, and release partially acquired objects in `unload`. Resource lifecycle handling does not make external effects transactional.

## Source and tests {#source-and-tests}

- [Public registration API]({{source-root}}/yt/yt/flow/library/python/companion/_api.py) and [registration tests]({{source-root}}/yt/yt/flow/library/python/companion/test/test_api.py).
- [Lifecycle implementation]({{source-root}}/yt/yt/flow/library/python/companion/resource.py) and [resource-store tests]({{source-root}}/yt/yt/flow/library/python/companion/test/test_resource_store.py), including resources visible to processing callbacks, stale batch references and deferred cleanup.
- [End-to-end resource test]({{source-root}}/yt/yt/flow/tests/companion/resource/python/test_resource.py): the shared scenario checks dependency values, dynamic updates and recovery after companion restart.
