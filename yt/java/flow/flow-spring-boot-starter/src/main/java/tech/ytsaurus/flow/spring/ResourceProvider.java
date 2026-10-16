package tech.ytsaurus.flow.spring;

import java.util.Map;
import java.util.function.Supplier;

import tech.ytsaurus.flow.resource.FlowResource;

/**
 * Provider interface for companion-hosted resource classes in Spring Boot applications.
 * <p>
 * Implement this interface and register it as a Spring bean to declare the companion resource
 * implementation types of a Flow pipeline in one place. Each explicit map key is registered via
 * {@link tech.ytsaurus.flow.context.PipelineContext#registerResourceClass(String, Supplier)}.
 * Resource ids remain in the pipeline spec; {@code FlowResourceClass} is not required or inspected.
 * <p>
 * The provider may be a singleton, but each supplied factory must create a fresh resource whenever
 * Flow requests a new instance. Do not return a cached singleton resource. Flow, not Spring, drives
 * its load/reconfigure/unload hooks. Collecting registrations does not invoke these factories.
 * <p>
 * Example usage:
 * <pre>
 * &#64;Configuration
 * public class MyFlowConfig implements ResourceProvider {
 *
 *     &#64;Override
 *     public Map&lt;String, Supplier&lt;? extends FlowResource&gt;&gt; getResourceClasses() {
 *         return Map.of("MyDictionary", MyDictionaryResource::new);
 *     }
 * }
 * </pre>
 *
 * @see FlowAutoConfiguration
 * @see ComputationProvider
 * @see FlowResource
 */
public interface ResourceProvider {

    /**
     * Returns the companion resource factories to register with the Flow pipeline.
     * <p>
     * Each key is the companion resource class name the pipeline spec uses under the
     * {@code companion_resource_class} parameter of a resource spec; the factory is called without
     * arguments whenever Flow needs a new instance, not for a no-op repeated init.
     *
     * @return Resource factories keyed by resource class name; must not be null.
     */
    Map<String, Supplier<? extends FlowResource>> getResourceClasses();

}
