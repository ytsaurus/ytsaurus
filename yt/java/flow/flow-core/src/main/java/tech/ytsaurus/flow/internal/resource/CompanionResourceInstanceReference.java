package tech.ytsaurus.flow.internal.resource;

import java.util.Objects;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.GUID;

/**
 * Exact resource instance required by a job or another companion resource.
 *
 * @param resourceId              the resource id from the pipeline spec.
 * @param incarnationId           the incarnation id of the required instance.
 * @param configurationGeneration the configuration generation of the required instance.
 * @param alias                   the alias the instance is exposed under; {@code null} unless the
 *                                job or resource refers to it directly.
 */
public record CompanionResourceInstanceReference(
        String resourceId,
        GUID incarnationId,
        long configurationGeneration,
        @Nullable String alias
) {

    /**
     * Validates that the mandatory components are present.
     */
    public CompanionResourceInstanceReference {
        Objects.requireNonNull(resourceId, "resourceId must not be null");
        Objects.requireNonNull(incarnationId, "incarnationId must not be null");
    }
}
