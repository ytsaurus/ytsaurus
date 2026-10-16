package tech.ytsaurus.flow.service;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.internal.utils.FailureCollector;
import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.resource.ResourceContext;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * A successfully loaded resource and its dependency lease. The final owner runs unload exactly once.
 */
final class ResourceInstance {
    private static final Logger log = LoggerFactory.getLogger(ResourceInstance.class);

    private final FlowResource resource;
    private final String resourceId;
    private final YTreeNode parameters;
    private final ResourceLease dependencies;
    private final LifecycleGate lifecycle;
    // Guarded by lifecycle, including when publication ownership transfers to a command.
    private int references = 1;

    private ResourceInstance(
            FlowResource resource, ResourceContext context, ResourceLease dependencies, LifecycleGate lifecycle
    ) {
        this.resource = resource;
        this.resourceId = context.resourceId();
        this.parameters = context.parameters();
        this.dependencies = dependencies;
        this.lifecycle = lifecycle;
        lifecycle.instanceCreated();
    }

    static ResourceInstance load(
            String resourceId, Supplier<? extends FlowResource> factory, InitCommandArg arg,
            ResourceLease dependencies, LifecycleGate lifecycle
    ) throws Exception {
        @Nullable FlowResource resource = null;
        try {
            var context = new ResourceContext(resourceId, arg.specParameters(), arg.dynamicSpecParameters(),
                    dependencies.resources());
            resource = Objects.requireNonNull(factory.get(), "Resource factory returned null");
            resource.load(context);
            return new ResourceInstance(resource, context, dependencies, lifecycle);
        } catch (Throwable error) {
            var failures = new FailureCollector();
            failures.add(error);
            // Until a loaded instance takes ownership, this command owns all cleanup.
            if (resource != null) {
                FlowResource failedResource = resource;
                failures.tryRun(() -> unloadResource(failedResource, resourceId));
            }
            failures.tryRun(dependencies::close);
            throw failures.asException();
        }
    }

    FlowResource resource() {
        return resource;
    }

    void reconfigure(InitCommandArg arg) throws Exception {
        resource.reconfigure(new ResourceContext(resourceId, parameters, arg.dynamicSpecParameters(),
                dependencies.resources()));
    }

    // Called only while the store holds lifecycle for an acquisition or publication.
    void retainLocked() {
        assert Thread.holdsLock(lifecycle);
        if (references == 0) {
            throw new IllegalStateException("Cannot retain a released resource");
        }
        ++references;
    }

    void release() {
        synchronized (lifecycle) {
            if (references == 0) {
                throw new IllegalStateException("Resource reference already released");
            }
            if (--references != 0) {
                return;
            }
        }
        var failures = new FailureCollector();
        try {
            failures.tryRun(() -> unloadResource(resource, resourceId));
            failures.tryRun(dependencies::close);
        } finally {
            // Remain live through both the hook and dependency cleanup, including retired instances.
            lifecycle.instanceDropped();
        }
        failures.throwUncheckedIfAny();
    }

    static void releaseAll(List<ResourceInstance> instances) {
        var failures = new FailureCollector();
        for (ResourceInstance instance : instances) {
            failures.tryRun(instance::release);
        }
        failures.throwUncheckedIfAny();
    }

    private static void unloadResource(FlowResource resource, String resourceId) {
        try {
            resource.unload();
        } catch (Exception error) {
            log.warn("Companion resource unload hook failed: (ResourceId: {})", resourceId, error);
        }
    }
}
