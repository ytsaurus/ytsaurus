package tech.ytsaurus.flow.resource;

/**
 * An expected failure while initializing a companion resource.
 */
public class ResourceLoadException extends Exception {
    /**
     * Creates a load failure with a diagnostic message.
     *
     * @param message the description of the failure.
     */
    public ResourceLoadException(String message) {
        super(message);
    }

    /**
     * Creates a load failure preserving the original cause.
     *
     * @param message the description of the failure.
     * @param cause the underlying failure.
     */
    public ResourceLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
