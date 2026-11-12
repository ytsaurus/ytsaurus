# Companion resources in Java and Kotlin

A companion resource holds an object shared by the jobs served by one JVM process, such as a client or reference data. Flow manages its creation, configuration and cleanup. Java and Kotlin use the same API from `tech.ytsaurus.flow.resource`.

Configure the [JVM companion](getting-started.md) first. The [common wiring reference](../concepts/companion.md#companion-resources) describes the worker proxy, resource dependencies and computation bindings.

## Register a factory {#registration}

Implement `FlowResource` and register a zero-argument `Supplier` on the `PipelineContext` before starting the companion:

- `registerResourceClass(String resourceClassName, Supplier<? extends FlowResource> factory)` registers an explicit implementation name.
- `registerResourceClass(Class<T> resourceClass, Supplier<? extends T> factory)` derives the implementation name from the class.

The factory must create a fresh instance on every call. Different resource ids and replacement instances have independent lifecycles, and a replacement can coexist with the previous instance while a batch still holds it. Registration does not invoke the factory. Repeating an already applied initialization does not create another instance. Blank or duplicate registration names are rejected.

Class-based registration uses the nonempty `@FlowResourceClass` value, or the fully qualified class name when the annotation is absent or empty. Put this name in `parameters.companion_resource_class`. The annotation changes naming only: registration is still required. Explicit string registration uses the supplied string.

The implementation name is distinct from the id in `spec.resources` and the alias used by a computation. See the maintained [resource companion]({{source-root}}/yt/yt/flow/tests/companion/resource/java/companion) and its [pipeline spec]({{source-root}}/yt/yt/flow/tests/companion/resource/java/companion/src/main/resources/pipeline.yson).

## Lifecycle and context {#lifecycle}

#|
|| **Method** | **Contract** ||
|| `load(ResourceContext context)` | Initialize a fresh instance from its parameters and dependencies. Wrap expected checked failures in `ResourceLoadException`, preserving the cause. ||
|| `reconfigure(ResourceContext context)` | Apply changed dynamic parameters to the live instance. The default implementation leaves it unchanged. ||
|| `unload()` | Release held objects and external resources, including after a partially completed `load`. Every implementation must define this method; a resource with nothing to release can use an empty implementation. ||
|#

`ResourceContext` exposes `resourceId()`, `parameters()`, `dynamicParameters()` and `dependencies()`. Static and dynamic parameters are `YTreeNode` values; absent dynamic parameters produce an empty map node. Static parameters include `companion_resource_class`.

The dependency map uses each dependency's alias, or its resource id when no alias is set. `context.dependency(alias, ResourceType.class)` checks the expected type; a missing alias or wrong type raises `IllegalStateException`. Dependencies remain held while the dependent instance lives.

## Read a resource in user code {#access}

Call `ctx.getResource(alias)` or `ctx.getResource(alias, ResourceType.class)` from your processing callback. The alias comes from the computation's `required_resource_ids`; without an alias, use the resource id. A serving context raises `IllegalStateException` for a missing binding or a type mismatch. Other `RuntimeContext` implementations can leave resource lookup unsupported.

Resolve the resource again for every batch. Do not cache it in a computation across batches: a later batch can require a different instance, and Flow can already have called `unload` on the cached one.

## Concurrency and failures {#concurrency-and-failures}

The same instance can serve multiple jobs and threads concurrently. Synchronize your mutable state. `reconfigure` refuses new acquisitions until it returns, but can overlap batches and dependent resources that already acquired the instance. A batch lease postpones `unload`; it does not serialize mutation or freeze configuration.

Flow attempts cleanup when `load` fails after construction. A failed `reconfigure` removes the instance from service; a subsequent initialization rebuilds it with `load`. Existing holders postpone cleanup until they release it. An `Exception` from `unload` is logged and swallowed; other failures propagate.

The worker [retries initialization](../concepts/companion.md#companion-resource-failures). Keep initialization safe to repeat and cleanup safe after partial initialization. Resource lifecycle handling does not make external effects transactional.

## Spring Boot and Kotlin {#spring-and-kotlin}

For [Spring Boot](spring.md), implement `ResourceProvider` and register the provider as a Spring bean. Its `getResourceClasses()` returns `Map<String, Supplier<? extends FlowResource>>`. Each key is the exact implementation name used by `companion_resource_class`; `@FlowResourceClass` is not inspected for this map.

The provider can be a singleton, but its factories must return fresh resources. Flow calls the lifecycle hooks, so returning a cached Spring singleton resource is unsuitable.

[Kotlin](../kotlin/getting-started.md) uses these same interfaces, factories, annotations and spec bindings. No separate resource API is required.

## Source and tests {#source-and-tests}

- [Resource interfaces]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/resource) and [PipelineContext]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/PipelineContext.java).
- [Resource store tests]({{source-root}}/yt/java/flow/flow-server/src/test/java/tech/ytsaurus/flow/service/ResourceStoreTest.java): fresh factories, parameter delivery, reconfiguration, leases and failures.
- [Spring ResourceProvider]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/ResourceProvider.java).
- [Integration test]({{source-root}}/yt/yt/flow/tests/companion/resource/java/test_resource.py): dependencies, dynamic updates and companion restart recovery.
