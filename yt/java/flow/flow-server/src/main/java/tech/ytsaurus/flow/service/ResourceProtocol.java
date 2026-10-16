package tech.ytsaurus.flow.service;

import java.util.Arrays;
import java.util.List;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.internal.resource.CompanionResourceInstanceReference;
import tech.ytsaurus.flow.rpc.EResourceExecuteStatus;

/**
 * Worker protocol history. Contains no live resource objects and performs no user-code calls.
 */
record ResourceProtocol(@Nullable Incarnation incarnation, @Nullable AppliedConfiguration applied) {
    static final ResourceProtocol EMPTY = new ResourceProtocol(null, null);

    record Incarnation(GUID id, long generation, boolean retired) {
    }

    record AppliedConfiguration(
            long generation,
            AppliedSpecs specs,
            List<CompanionResourceInstanceReference> dependencies
    ) {
    }

    sealed interface InitDecision {
    }

    record Reply(ExecuteOutcome outcome) implements InitDecision {
    }

    /**
     * History to install before calling the factory, even if loading later fails.
     */
    record Create(ResourceProtocol history) implements InitDecision {
    }

    record Reconfigure() implements InitDecision {
    }

    InitDecision decideInit(
            String resourceId, InitCommandArg request, AppliedSpecs requestedSpecs, boolean resourceReady
    ) {
        if (incarnation == null) {
            return createNewIncarnation(request);
        }

        long requestedGeneration = request.incarnationGeneration();
        long currentGeneration = incarnation.generation();

        // An older generation must not replace the current incarnation.
        if (requestedGeneration < currentGeneration) {
            return staleIncarnation(resourceId, request, incarnation);
        }

        boolean sameIncarnationId = request.incarnationId().equals(incarnation.id());

        // Equal generations must name the same incarnation.
        if (requestedGeneration == currentGeneration) {
            if (!sameIncarnationId) {
                return staleIncarnation(resourceId, request, incarnation);
            }
        }

        // A retired id cannot be revived, even by a request with a larger generation.
        if (sameIncarnationId) {
            if (incarnation.retired()) {
                return staleIncarnation(resourceId, request, incarnation);
            }
        }

        if (requestedGeneration > currentGeneration) {
            return createNewIncarnation(request);
        }

        // The request belongs to the current, non-retired incarnation.
        return decideConfiguration(resourceId, request, requestedSpecs, resourceReady);
    }

    ResourceProtocol applied(InitCommandArg request, AppliedSpecs specs) {
        var configuration = new AppliedConfiguration(
                request.configurationGeneration(), specs, List.copyOf(request.dependencies()));
        return new ResourceProtocol(incarnation, configuration);
    }

    ResourceProtocol unload(GUID incarnationId) {
        if (incarnation == null) {
            // Unload carries no generation; remember its id to reject a late init of that id.
            var retiredIncarnation = new Incarnation(incarnationId, 0, true);
            return new ResourceProtocol(retiredIncarnation, null);
        }
        if (incarnation.retired()) {
            return this;
        }
        if (!incarnation.id().equals(incarnationId)) {
            // A late unload of another incarnation must not retire the current one.
            return this;
        }

        var retiredIncarnation = new Incarnation(incarnation.id(), incarnation.generation(), true);
        return new ResourceProtocol(retiredIncarnation, null);
    }

    private InitDecision decideConfiguration(
            String resourceId, InitCommandArg request, AppliedSpecs requestedSpecs, boolean resourceReady
    ) {
        long appliedGeneration = 0;
        if (applied != null) {
            appliedGeneration = applied.generation();
        }
        long requestedGeneration = request.configurationGeneration();

        // Older requests never roll back a successfully applied configuration.
        if (requestedGeneration < appliedGeneration) {
            if (resourceReady) {
                return new Reply(ExecuteOutcome.ok());
            }
            return reply(EResourceExecuteStatus.RES_RESOURCE_NOT_INITIALIZED,
                    "Resource '%s' is not initialized at configuration generation %s".formatted(
                            resourceId, appliedGeneration));
        }

        if (applied != null) {
            // Static specs stay fixed within an incarnation, even after its instance fails.
            if (!Arrays.equals(applied.specs().spec(), requestedSpecs.spec())) {
                return reply(EResourceExecuteStatus.RES_ERROR,
                        "Static resource spec changed within resource '%s' incarnation %s".formatted(
                                resourceId, request.incarnationId()));
            }
        }

        List<CompanionResourceInstanceReference> appliedDependencies = List.of();
        if (applied != null) {
            appliedDependencies = applied.dependencies();
        }
        boolean sameDependencies = request.dependencies().equals(appliedDependencies);
        boolean sameConfigurationGeneration = requestedGeneration == appliedGeneration;

        if (sameConfigurationGeneration) {
            // Only a successful load/reconfigure binds the payload of this generation.
            if (applied != null) {
                if (!Arrays.equals(applied.specs().dynamicSpec(), requestedSpecs.dynamicSpec())) {
                    return conflict(resourceId, request, "dynamic specs");
                }
                if (!Arrays.equals(applied.specs().resourceRevision(), requestedSpecs.resourceRevision())) {
                    return conflict(resourceId, request, "revisions");
                }
            }
        }

        // Rebuild an unavailable instance, retaining its incarnation's spec history.
        if (!resourceReady) {
            return new Create(this);
        }

        // Changed dependencies require a fresh instance, even at the same configuration generation.
        if (!sameDependencies) {
            return new Create(this);
        }

        // The instance already serves the requested configuration and dependencies.
        if (sameConfigurationGeneration) {
            return new Reply(ExecuteOutcome.ok());
        }

        // Only a newer configuration of a ready instance can be applied in place.
        return new Reconfigure();
    }

    private static Create createNewIncarnation(InitCommandArg request) {
        var nextIncarnation = new Incarnation(request.incarnationId(), request.incarnationGeneration(), false);
        var newHistory = new ResourceProtocol(nextIncarnation, null);
        return new Create(newHistory);
    }

    private static Reply staleIncarnation(String resourceId, InitCommandArg request, Incarnation current) {
        return reply(EResourceExecuteStatus.RES_STALE_RESOURCE_INCARNATION,
                "Resource '%s' incarnation %s is stale; current incarnation is %s".formatted(
                        resourceId, request.incarnationId(), current.id()));
    }

    private static Reply conflict(String resourceId, InitCommandArg request, String what) {
        return reply(EResourceExecuteStatus.RES_ERROR,
                "Resource '%s' incarnation %s has conflicting %s at configuration generation %s".formatted(
                        resourceId, request.incarnationId(), what, request.configurationGeneration()));
    }

    private static Reply reply(EResourceExecuteStatus status, String message) {
        return new Reply(new ExecuteOutcome(status, message));
    }
}
