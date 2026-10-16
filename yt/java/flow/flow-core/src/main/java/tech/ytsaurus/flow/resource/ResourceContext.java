package tech.ytsaurus.flow.resource;

import java.util.Map;
import java.util.Objects;

import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Everything a companion resource sees at load and reconfigure time.
 *
 * @param resourceId        the resource id from the pipeline spec.
 * @param parameters        the {@code parameters} map of the static spec; it also carries the
 *                          {@code companion_resource_class} key.
 * @param dynamicParameters the {@code parameters} map of the dynamic spec; an empty map node when
 *                          the dynamic spec carries none.
 * @param dependencies      initialized dependency instances keyed by the alias from the dependency
 *                          reference (the dependency resource id when no alias is set).
 */
public record ResourceContext(
        String resourceId,
        YTreeNode parameters,
        YTreeNode dynamicParameters,
        Map<String, FlowResource> dependencies
) {

    /**
     * Validates that all components are present.
     */
    public ResourceContext {
        Objects.requireNonNull(resourceId, "resourceId must not be null");
        Objects.requireNonNull(parameters, "parameters must not be null");
        Objects.requireNonNull(dynamicParameters, "dynamicParameters must not be null");
        Objects.requireNonNull(dependencies, "dependencies must not be null");
    }

    /**
     * Dependency instance by alias, checked against the expected type.
     *
     * @param alias The alias from the dependency entry (the resource id when no alias is set).
     * @param type  The expected resource type.
     * @param <T>   The expected resource type.
     * @return The initialized dependency instance.
     * @throws IllegalStateException If no dependency with such alias exists, or the bound
     *                               dependency is not of the expected type.
     */
    public <T extends FlowResource> T dependency(String alias, Class<T> type) {
        FlowResource dependency = dependencies.get(alias);
        if (dependency == null) {
            throw new IllegalStateException(
                    "Resource '%s' has no dependency aliased '%s'".formatted(resourceId, alias));
        }
        if (!type.isInstance(dependency)) {
            throw new IllegalStateException(
                    "Dependency '%s' of resource '%s' is a %s, not the expected %s".formatted(
                            alias, resourceId, dependency.getClass().getName(), type.getName()));
        }
        return type.cast(dependency);
    }
}
