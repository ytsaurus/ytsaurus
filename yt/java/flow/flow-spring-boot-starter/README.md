# Spring Boot Starter for YT Flow

The `flow-spring-boot-starter` module provides Spring Boot auto-configuration for easy integration with Spring applications.

## Setup

1. Add the `flow-spring-boot-starter` dependency to your project.
2. Implement `ProcessFunction`.

```java
@FlowComputation(id="mapper")
public class WordCountMapper implements RowFunction {

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        // User-defined processing logic.
    }
}
```

3. Create SpringBootApplication.

```java
@SpringBootApplication
public class WordCountApplication {
    public static void main(String[] args) {
        new SpringApplicationBuilder(WordCountApplication.class)
                .run(args);
    }
}
```

4. In Worker mode, the gRPC server starts when the Spring application context is ready.

## Companion resource classes

Declare resource implementation factories through a `ResourceProvider` bean. The map keys are
implementation type names, not pipeline resource ids or Spring bean names:

```java
@Configuration(proxyBeanMethods = false)
class ResourceConfiguration {
    @Bean
    ResourceProvider resourceClasses() {
        return () -> Map.of(
                "GreetingResource", GreetingResource::new
        );
    }
}
```

The starter registers each entry through `PipelineContext.registerResourceClass(String, Supplier)`.
`GreetingResource` needs neither `@FlowResourceClass` nor `@Component`; the provider's explicit key
is authoritative. The starter does not scan `@FlowResourceClass` for automatic registration.
Duplicate type names from different providers are rejected by the same core registration checks.

The pipeline specification still declares the actual resources. For example, two resource ids
`greeting_en` and `greeting_ru` may both select `GreetingResource` through
`parameters.companion_resource_class`, with separate parameters and lifecycles. Computations look
up their resources by the aliases from `required_resource_ids`, not by Spring injection. See the
[SDK registration guide](../README.md#companion-resources) for the three distinct names.

The provider bean can be a **singleton factory registry**, but its suppliers must return fresh
resource instances whenever Flow requests creation. Collecting the map does not invoke its
suppliers. Do not capture an injected singleton `FlowResource` and return it repeatedly: different
resource ids or incarnations must not share the same lifecycle object. Put resource initialization
in `load`, where the worker's parameters and resource dependencies are available, and cleanup in
`unload`.

Implement `load(ResourceContext) throws ResourceLoadException` and explicitly implement
`unload() throws Exception`. Preserve the cause when wrapping an initialization failure. Flow calls
unload after partial load failures too, so cleanup must handle fields that were not initialized.
A retry obtains a fresh resource from the supplier. Flow owns the returned resource instances, not
the lifetime of the singleton `ResourceProvider` bean. See the [SDK lifecycle example](../README.md#loading-and-cleanup).

If full Spring bean creation is needed, a supplier can call an `ObjectProvider` for a prototype bean
without a scoped proxy. `ObjectProvider` alone does not change singleton scope into prototype scope,
and Spring does not automatically invoke destruction callbacks on prototypes; Flow's `unload` must
release what that resource owns. See [Spring bean scopes](https://docs.spring.io/spring-framework/reference/core/beans/factory-scopes.html).

## Configuration Properties

You can configure the Flow server using Spring configuration properties in `application.yml` or `application.properties`:

```yaml
flow:
    server:
        port: 8080  # Optional, defaults to the port field in YT_FLOW_COMPANION_CONFIG
```

| Property           | Description                                             | Default                                                    |
|--------------------|---------------------------------------------------------|------------------------------------------------------------|
| `flow.server.port` | The port on which the gRPC companion server will listen | `port` field from `YT_FLOW_COMPANION_CONFIG` YSON payload |
