package tech.ytsaurus.flow.resource;

/**
 * Derives the spec-facing name of a companion resource class.
 */
public final class FlowResourceAnnotations {

    private FlowResourceAnnotations() {
    }

    /**
     * Returns the name the pipeline spec uses under the {@code companion_resource_class} parameter
     * for the given class: the non-empty {@link FlowResourceClass} annotation value if present,
     * else the fully-qualified class name.
     *
     * @param resourceClass the resource class to name
     * @return the spec-facing resource class name
     */
    public static String resourceClassName(Class<? extends FlowResource> resourceClass) {
        FlowResourceClass annotation = resourceClass.getAnnotation(FlowResourceClass.class);
        if (annotation != null && !annotation.value().isEmpty()) {
            return annotation.value();
        }
        return resourceClass.getName();
    }
}
