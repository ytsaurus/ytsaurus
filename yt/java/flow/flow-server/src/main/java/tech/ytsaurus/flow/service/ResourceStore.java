package tech.ytsaurus.flow.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.google.protobuf.ByteString;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.internal.resource.CompanionResourceInstanceReference;
import tech.ytsaurus.flow.internal.utils.FailureCollector;
import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.rpc.EResourceCommand;
import tech.ytsaurus.flow.rpc.EResourceExecuteStatus;

/**
 * Hosts resources shared by the jobs of one companion runtime.
 *
 * <p>Worker protocol history, ready publications and physical instances have separate lifetimes.
 * Commands serialize per resource id; batches never wait for that command lock. The lifecycle
 * monitor guards the registry and publication/lease handoff, never user hooks.
 */
public class ResourceStore {
    public static final String COMPANION_RESOURCE_CLASS_KEY = "companion_resource_class";

    private static final Logger log = LoggerFactory.getLogger(ResourceStore.class);
    private static final String STORE_SHUT_DOWN_MESSAGE = "Companion resource store is shut down";

    private final Map<String, Supplier<? extends FlowResource>> resourceFactories;
    private final LifecycleGate lifecycle = new LifecycleGate();
    // Both the map and slot contents are guarded by lifecycle.
    private final Map<String, ResourceSlot> slots = new HashMap<>();

    public ResourceStore(Map<String, Supplier<? extends FlowResource>> resourceFactories) {
        this.resourceFactories = Map.copyOf(Objects.requireNonNull(resourceFactories));
    }

    /**
     * Executes a worker command. Expected failures are returned in-band; Errors propagate after cleanup.
     */
    public ExecuteOutcome execute(String resourceId, int commandValue, @Nullable ByteString argument) {
        EResourceCommand command = EResourceCommand.forNumber(commandValue);
        if (command == null) {
            return new ExecuteOutcome(EResourceExecuteStatus.RES_UNSUPPORTED,
                    "Unsupported resource command %s".formatted(commandValue));
        }
        ResourceSlot slot;
        synchronized (lifecycle) {
            if (lifecycle.isClosed()) {
                return closedOutcome();
            }
            slot = slots.computeIfAbsent(resourceId, id -> new ResourceSlot());
        }
        if (slot.commands.isHeldByCurrentThread()) {
            return new ExecuteOutcome(EResourceExecuteStatus.RES_ERROR,
                    "Resource command re-entered from a hook of resource '%s'".formatted(resourceId));
        }
        slot.commands.lock();
        try {
            if (!lifecycle.tryEnterCommand()) {
                return closedOutcome();
            }
            try {
                return switch (command) {
                    case RC_INIT -> init(resourceId, slot, argument);
                    case RC_UNLOAD -> unload(slot, argument);
                };
            } finally {
                lifecycle.leaveCommand();
            }
        } finally {
            slot.commands.unlock();
        }
    }

    /**
     * Acquires every exact reference, exposing only aliased resources. Returns null if closed or
     * any reference is unavailable. The lease protects lifetime, not configuration immutability.
     */
    public @Nullable ResourceLease acquire(List<CompanionResourceInstanceReference> references) {
        return acquire(references, false);
    }

    private @Nullable ResourceLease acquire(
            List<CompanionResourceInstanceReference> references, boolean dependencies
    ) {
        if (references.isEmpty()) {
            return lifecycle.isClosed() ? null : ResourceLease.EMPTY;
        }
        synchronized (lifecycle) {
            if (lifecycle.isClosed()) {
                return null;
            }
            Map<String, FlowResource> resources = new HashMap<>();
            List<ResourceInstance> instances = new ArrayList<>(references.size());
            for (var reference : references) {
                ResourceSlot slot = slots.get(reference.resourceId());
                ReadyResource ready = slot != null ? slot.ready : null;
                if (ready == null || !ready.matches(reference)) {
                    return null;
                }
                instances.add(ready.instance());
                String alias = reference.alias();
                if (alias != null || dependencies) {
                    resources.put(alias != null ? alias : reference.resourceId(), ready.instance().resource());
                }
            }
            // Validate and allocate before retaining anything. Publication cannot change until
            // every reference has been retained, so a failed acquisition needs no rollback hooks.
            var lease = new ResourceLease(resources, instances);
            instances.forEach(ResourceInstance::retainLocked);
            return lease;
        }
    }

    /**
     * Closes admission and retires all publications without waiting for command locks or leases.
     * Last-owner unload hooks run synchronously and may block. Other owners clean up on release.
     *
     * @return whether all admitted commands and all instance unload hooks have finished. False also
     *         accounts for instances retired before this call. Once true, it stays true.
     */
    public boolean shutdown() {
        List<ResourceInstance> retired;
        synchronized (lifecycle) {
            lifecycle.close();
            retired = new ArrayList<>(slots.size());
            for (ResourceSlot slot : slots.values()) {
                ResourceInstance instance = withdraw(slot);
                if (instance != null) {
                    retired.add(instance);
                }
            }
            slots.clear();
        }
        ResourceInstance.releaseAll(retired);
        return lifecycle.isQuiescent();
    }

    private ExecuteOutcome init(String resourceId, ResourceSlot slot, @Nullable ByteString argument) {
        try {
            InitCommandArg arg = InitCommandArg.parse(InitCommandArg.parseArgument(argument));
            AppliedSpecs specs = arg.canonicalSpecs();
            ResourceProtocol history;
            boolean ready;
            synchronized (lifecycle) {
                if (lifecycle.isClosed()) {
                    return closedOutcome();
                }
                history = slot.protocol;
                ready = slot.ready != null;
            }
            // Commands on this slot are serialized. Compare potentially large spec payloads
            // outside the registry monitor; shutdown is checked again before any state change.
            var decision = history.decideInit(resourceId, arg, specs, ready);
            ResourceInstance replaced = null;
            if (decision instanceof ResourceProtocol.Create create && create.history() != history) {
                synchronized (lifecycle) {
                    if (lifecycle.isClosed()) {
                        return closedOutcome();
                    }
                    // Fence old commands even if the new incarnation's factory or load fails.
                    slot.protocol = create.history();
                    replaced = withdraw(slot);
                }
            }
            if (replaced != null) {
                replaced.release();
            }
            if (decision instanceof ResourceProtocol.Reply reply) {
                return reply.outcome();
            }
            if (decision instanceof ResourceProtocol.Create) {
                return create(resourceId, slot, arg, specs);
            }
            return reconfigure(resourceId, slot, arg, specs);
        } catch (Exception error) {
            log.warn("Companion resource init failed: (ResourceId: {})", resourceId, error);
            return new ExecuteOutcome(EResourceExecuteStatus.RES_ERROR, errorMessage(error));
        }
    }

    private ExecuteOutcome create(String resourceId, ResourceSlot slot, InitCommandArg arg, AppliedSpecs specs)
            throws Exception {
        String className = arg.companionResourceClass();
        Supplier<? extends FlowResource> factory = resourceFactories.get(className);
        if (factory == null) {
            return new ExecuteOutcome(EResourceExecuteStatus.RES_RESOURCE_NOT_FOUND,
                    ("Companion has no factory for resource class '%s'; "
                            + "declare it via PipelineContext.registerResourceClass").formatted(className));
        }
        ResourceInstance previous;
        synchronized (lifecycle) {
            if (lifecycle.isClosed()) {
                return closedOutcome();
            }
            previous = withdraw(slot);
        }
        if (previous != null) {
            previous.release();
        }
        log.info("Loading companion resource: (ResourceId: {}, ResourceClass: {})", resourceId, className);
        ResourceLease dependencies = acquire(arg.dependencies(), true);
        if (dependencies == null) {
            return new ExecuteOutcome(EResourceExecuteStatus.RES_RESOURCE_NOT_INITIALIZED,
                    "Companion dependencies are not initialized for resource '%s': %s".formatted(
                            resourceId, arg.dependencies()));
        }
        ResourceInstance candidate = ResourceInstance.load(resourceId, factory, arg, dependencies, lifecycle);
        return FailureCollector.callWithCleanup(() -> publish(slot, candidate, arg, specs), candidate::release);
    }

    private ExecuteOutcome reconfigure(String resourceId, ResourceSlot slot, InitCommandArg arg, AppliedSpecs specs)
            throws Exception {
        ResourceInstance instance;
        synchronized (lifecycle) {
            if (lifecycle.isClosed()) {
                return closedOutcome();
            }
            // Transfer publication ownership to this command. Shutdown cannot unload its instance
            // during the hook, and new batches cannot acquire a partially reconfigured resource.
            instance = Objects.requireNonNull(withdraw(slot), "Reconfigure requires a ready resource");
        }
        return FailureCollector.callWithCleanup(() -> {
            log.info("Reconfiguring companion resource: (ResourceId: {}, ConfigurationGeneration: {})",
                    resourceId, arg.configurationGeneration());
            instance.reconfigure(arg);
            return publish(slot, instance, arg, specs);
        }, instance::release);
    }

    private ExecuteOutcome publish(
            ResourceSlot slot, ResourceInstance instance, InitCommandArg arg, AppliedSpecs specs
    ) {
        synchronized (lifecycle) {
            if (lifecycle.isClosed()) {
                return closedOutcome();
            }
            var history = slot.protocol.applied(arg, specs);
            var ready = new ReadyResource(arg.incarnationId(), arg.configurationGeneration(), instance);
            // The publication takes its own reference; the command releases its reference on exit.
            instance.retainLocked();
            slot.protocol = history;
            slot.ready = ready;
        }
        return ExecuteOutcome.ok();
    }

    private ExecuteOutcome unload(ResourceSlot slot, @Nullable ByteString argument) {
        GUID incarnationId;
        try {
            incarnationId = InitCommandArg.parseUnload(argument);
        } catch (Exception error) {
            return new ExecuteOutcome(EResourceExecuteStatus.RES_ERROR, errorMessage(error));
        }
        ResourceInstance retired;
        synchronized (lifecycle) {
            if (lifecycle.isClosed()) {
                return closedOutcome();
            }
            ResourceProtocol history = slot.protocol.unload(incarnationId);
            if (history == slot.protocol) {
                return ExecuteOutcome.ok();
            }
            slot.protocol = history;
            retired = withdraw(slot);
        }
        if (retired != null) {
            retired.release();
        }
        return ExecuteOutcome.ok();
    }

    // Caller holds lifecycle and takes over the publication's owning reference.
    private static @Nullable ResourceInstance withdraw(ResourceSlot slot) {
        ReadyResource ready = slot.ready;
        slot.ready = null;
        return ready != null ? ready.instance() : null;
    }

    private static ExecuteOutcome closedOutcome() {
        return new ExecuteOutcome(EResourceExecuteStatus.RES_ERROR, STORE_SHUT_DOWN_MESSAGE);
    }

    private static String errorMessage(Exception error) {
        String message = error.getMessage();
        return message != null && !message.isEmpty() ? message : error.toString();
    }

    private record ReadyResource(GUID incarnationId, long configurationGeneration, ResourceInstance instance) {
        boolean matches(CompanionResourceInstanceReference reference) {
            return incarnationId.equals(reference.incarnationId())
                    && configurationGeneration == reference.configurationGeneration();
        }
    }

    private static final class ResourceSlot {
        final ReentrantLock commands = new ReentrantLock();
        ResourceProtocol protocol = ResourceProtocol.EMPTY;
        @Nullable ReadyResource ready;
    }
}
